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
my @sources = (
	'src/backend/utils/init/backend_runtime.c',
	'src/backend/utils/cache/backend_runtime_cache.c',
	'src/backend/utils/activity/backend_runtime_pgstat.c',
	'src/backend/jit/backend_runtime_jit.c',
	'src/backend/storage/buffer/backend_runtime_buffer.c',
	'src/backend/storage/file/backend_runtime_file.c',
	'src/backend/storage/lmgr/backend_runtime_lmgr.c',
	'src/backend/storage/ipc/backend_runtime_ipc.c',
	'src/backend/storage/ipc/ipc.c');
my $help = 0;

GetOptions(
	'header=s' => \$header,
	'manifest=s' => \$manifest,
	'source=s' => \@sources,
	'help' => \$help,
) or usage(2);

usage(0) if $help;

my @fields = read_runtime_fields($header);
my %header_fields = map { field_key($_) => $_ } @fields;
my @manifest_rows = read_manifest($manifest);
my $source_text = read_sources(@sources);
my %source_functions = defined_functions($source_text);
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

	foreach my $column (qw(initializer early_adoption reset_destroy))
	{
		foreach my $function (runtime_function_refs($row->{$column}))
		{
			if (!exists $source_functions{$function})
			{
				push @errors,
				  "$manifest:$row->{line}: $column references $function(), but no definition was found in the checked runtime sources";
			}
		}
	}
}

foreach my $field (@fields)
{
	my $key = field_key($field);

	push @errors, "missing lifecycle row for $key"
	  unless exists $manifest_fields{$key};
}

push @errors, require_function_calls(
	'InitializePgProcessRuntime',
	[qw(PgBackendInitializeRuntimeObject
		PgSessionInitializeRuntimeObject
		PgConnectionInitializeRuntimeObject
		PgExecutionInitializeRuntimeObject
		PgBackendAdoptEarlyState
		PgSessionAdoptEarlyState
		PgConnectionAdoptEarlyState
		PgExecutionAdoptEarlyState)]);

push @errors, require_function_calls(
	'InitializePgThreadBackendRuntimeState',
	[qw(PgBackendInitializeRuntimeObject
		PgSessionInitializeRuntimeObject
		PgConnectionInitializeRuntimeObject
		PgExecutionInitializeRuntimeObject)]);

push @errors, require_function_calls(
	'InstallPgThreadBackendRuntimeState',
	[qw(PgBackendAdoptEarlyState
		PgSessionAdoptEarlyState
		PgConnectionAdoptEarlyState
		PgExecutionAdoptEarlyState)]);

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
  --source FILE     runtime source path to scan for lifecycle functions
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

sub read_sources
{
	my (@files) = @_;
	my $text = '';

	foreach my $file (@files)
	{
		open my $fh, '<', $file or die "could not open $file: $!";
		local $/;
		$text .= "\n" . <$fh>;
	}

	return $text;
}

sub defined_functions
{
	my ($text) = @_;
	my %functions;

	while ($text =~ /^([A-Za-z_][A-Za-z0-9_]*)\s*\(/mg)
	{
		$functions{$1} = 1;
	}

	return %functions;
}

sub runtime_function_refs
{
	my ($text) = @_;
	my %refs;

	while ($text =~ /\b((?:PgRuntime|PgBackend|PgSession|PgConnection|PgExecution|InitializePg)[A-Za-z0-9_]*)\s*\(/g)
	{
		$refs{$1} = 1;
	}

	return sort keys %refs;
}

sub require_function_calls
{
	my ($function, $calls) = @_;
	my @errors;
	my $body = extract_function_body($source_text, $function);

	if (!defined $body)
	{
		return
		  "could not find required runtime lifecycle function $function()";
	}

	foreach my $call (@{$calls})
	{
		if ($body !~ /\b\Q$call\E\s*\(/)
		{
			push @errors,
			  "$function() must call $call() to keep process/thread runtime construction symmetrical";
		}
	}

	return @errors;
}

sub extract_function_body
{
	my ($text, $function) = @_;
	my @lines = split /\n/, $text;
	my $start;

	for my $idx (0 .. $#lines)
	{
		if ($lines[$idx] =~ /^\Q$function\E\s*\(/)
		{
			$start = $idx;
			last;
		}
	}

	return undef unless defined $start;

	my $body = '';
	my $brace_depth = 0;
	my $seen_open = 0;

	for my $idx ($start .. $#lines)
	{
		my $line = $lines[$idx];

		$body .= $line . "\n";
		my $opens = () = $line =~ /\{/g;
		my $closes = () = $line =~ /\}/g;
		$brace_depth += $opens;
		$seen_open = 1 if $opens;
		$brace_depth -= $closes;

		return $body if $seen_open && $brace_depth == 0;
	}

	return undef;
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
