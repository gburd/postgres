#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# check_runtime_lifecycles.pl
#	  Check runtime object lifecycle classification coverage.
#
# Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
#
# src/tools/runtime_lifecycle/check_runtime_lifecycles.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use Getopt::Long qw(GetOptions);

my $header = 'src/include/utils/backend_runtime.h';
my $manifest = 'MULTITHREADED_RUNTIME_LIFECYCLE.tsv';
my $help = 0;

GetOptions(
	'header=s' => \$header,
	'manifest=s' => \$manifest,
	'help' => \$help,
) or usage(2);

usage(0) if $help;

my @fields = read_runtime_fields($header);
my %header_fields = map { field_key($_) => $_ } @fields;
my @manifest_rows = read_manifest($manifest);
my %manifest_fields;
my @errors;

foreach my $row (@manifest_rows)
{
	my $key = field_key($row);

	if (exists $manifest_fields{$key})
	{
		push @errors,
		  "$manifest:$row->{line}: duplicate lifecycle row for $key";
		next;
	}

	$manifest_fields{$key} = $row;

	if (!exists $header_fields{$key})
	{
		push @errors,
		  "$manifest:$row->{line}: stale lifecycle row for $key";
	}
}

foreach my $field (@fields)
{
	my $key = field_key($field);

	push @errors, "missing lifecycle row for $key"
	  unless exists $manifest_fields{$key};
}

if (@errors)
{
	print "runtime lifecycle check failed:\n";
	print "  $_\n" foreach @errors;
	exit 1;
}

printf "runtime lifecycle check passed: %d fields classified\n",
  scalar @fields;
exit 0;

sub usage
{
	my ($status) = @_;

	print <<'USAGE';
Usage: perl src/tools/runtime_lifecycle/check_runtime_lifecycles.pl [OPTIONS]

Options:
  --header FILE     backend_runtime.h path
  --manifest FILE   lifecycle manifest path
  --help            show this help
USAGE

	exit $status;
}

sub field_key
{
	my ($row) = @_;

	return "$row->{object}.$row->{field}";
}

sub read_runtime_fields
{
	my ($file) = @_;

	open my $fh, '<', $file or die "could not open $file: $!";

	my @fields;
	my $object;
	my $in_object = 0;
	my %objects = map { $_ => 1 }
	  qw(PgBackend PgSession PgConnection PgExecution);

	while (my $line = <$fh>)
	{
		if ($line =~ /^struct\s+(PgBackend|PgSession|PgConnection|PgExecution)\s*$/)
		{
			$object = $1;
			$in_object = 1;
			next;
		}

		if ($in_object && $line =~ /^};/)
		{
			$in_object = 0;
			undef $object;
			next;
		}

		next unless $in_object;
		next if $line =~ /^\s*$/;
		next if $line =~ /^\s*(?:\/\*|\*)/;

		if ($line =~ /^\s*(?:struct\s+)?[A-Za-z_][A-Za-z0-9_]*\s*(?:\*+\s*)?([A-Za-z_][A-Za-z0-9_]*)\s*;/)
		{
			push @fields,
			  {
				object => $object,
				field => $1,
			  };
		}
	}

	return @fields;
}

sub read_manifest
{
	my ($file) = @_;

	open my $fh, '<', $file or die "could not open $file: $!";

	my @rows;
	while (my $line = <$fh>)
	{
		chomp $line;
		next if $line =~ /^\s*$/;
		next if $line =~ /^\s*#/;

		my @cols = split /\t/, $line, -1;
		if ($cols[0] eq 'object' && $cols[1] eq 'field')
		{
			next;
		}

		die "$file:$.: expected 8 tab-separated columns\n"
		  unless @cols == 8;

		for my $idx (0 .. 7)
		{
			die "$file:$.: column " . ($idx + 1) . " must not be empty\n"
			  if $cols[$idx] eq '';
		}

		push @rows,
		  {
			object => $cols[0],
			field => $cols[1],
			owner_lifetime => $cols[2],
			initializer => $cols[3],
			early_adoption => $cols[4],
			reset_destroy => $cols[5],
			copy_rule => $cols[6],
			notes => $cols[7],
			line => $.,
		  };
	}

	return @rows;
}
