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
my $owner_map = 'MULTITHREADED_RUNTIME_OWNERS.tsv';
my @sources = (
	'src/backend/utils/init/backend_runtime.c',
	'src/backend/utils/init/backend_runtime_backend.c',
	'src/backend/utils/init/backend_runtime_execution.c',
	'src/backend/utils/init/backend_runtime_session.c',
	'src/backend/utils/init/backend_runtime_teardown.c',
	'src/backend/tcop/backend_runtime_tcop.c',
	'src/backend/utils/cache/backend_runtime_cache.c',
	'src/backend/utils/activity/backend_runtime_pgstat.c',
	'src/backend/utils/activity/backend_status.c',
	'src/backend/utils/adt/backend_runtime_pseudorandom.c',
	'src/backend/utils/adt/backend_runtime_ri.c',
	'src/backend/utils/error/backend_runtime_error.c',
	'src/backend/utils/fmgr/backend_runtime_extension.c',
	'src/backend/utils/misc/backend_runtime_guc.c',
	'src/backend/utils/misc/backend_runtime_utility.c',
	'src/backend/utils/misc/timeout.c',
	'src/backend/utils/mb/backend_runtime_mb.c',
	'src/backend/utils/mmgr/backend_runtime_memory.c',
	'src/backend/utils/mmgr/backend_runtime_portal.c',
	'src/backend/regex/backend_runtime_regex.c',
	'src/backend/optimizer/util/backend_runtime_optimizer.c',
	'src/backend/commands/backend_runtime_async.c',
	'src/backend/commands/event_trigger.c',
	'src/backend/commands/backend_runtime_event_trigger.c',
	'src/backend/commands/backend_runtime_trigger.c',
	'src/backend/replication/logical/backend_runtime_logical.c',
	'src/backend/postmaster/interrupt.c',
	'src/backend/jit/backend_runtime_jit.c',
	'src/backend/access/transam/backend_runtime_parallel.c',
	'src/backend/access/transam/backend_runtime_xact.c',
	'src/backend/libpq/backend_runtime_connection.c',
	'src/backend/storage/buffer/backend_runtime_buffer.c',
	'src/backend/storage/file/backend_runtime_file.c',
	'src/backend/storage/lmgr/backend_runtime_lmgr.c',
	'src/backend/storage/ipc/backend_runtime_ipc.c',
	'src/backend/storage/ipc/dsm.c',
	'src/backend/storage/ipc/ipc.c');
my @bucket_defs = (
	'src/backend/utils/init/backend_runtime_runtime_buckets.def',
	'src/backend/utils/init/backend_runtime_backend_buckets.def',
	'src/backend/utils/init/backend_runtime_carrier_buckets.def',
	'src/backend/utils/init/backend_runtime_session_buckets.def',
	'src/backend/utils/init/backend_runtime_connection_buckets.def',
	'src/backend/utils/init/backend_runtime_execution_buckets.def');
my @reset_defs = (
	'src/backend/utils/init/backend_runtime_session_reset_buckets.def');
my $help = 0;

GetOptions(
	'header=s' => \$header,
	'manifest=s' => \$manifest,
	'owner-map=s' => \$owner_map,
	'source=s' => \@sources,
	'bucket-def=s' => \@bucket_defs,
	'reset-def=s' => \@reset_defs,
	'help' => \$help,
) or usage(2);

usage(0) if $help;

my @errors;
my @fields = read_runtime_fields($header);
my %header_fields = map { field_key($_) => $_ } @fields;
my @manifest_rows = read_manifest($manifest);
my @owner_rows = read_owner_map($owner_map);
my @bucket_rows = read_bucket_defs(@bucket_defs);
my @reset_rows = read_reset_defs(@reset_defs);
my $source_text = read_sources(@sources);
my %source_functions = defined_functions($source_text);
my %manifest_fields;
my %bucket_fields;
my %reset_fields;

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

validate_owner_map(\@owner_rows, \%manifest_fields, $header);

foreach my $row (@bucket_rows)
{
	my $key = field_key($row);

	if (exists $bucket_fields{$key})
	{
		push @errors,
		  "$row->{file}:$row->{line}: duplicate bucket definition for $key";
		next;
	}

	$bucket_fields{$key} = $row;

	if (!exists $header_fields{$key})
	{
		push @errors,
		  "$row->{file}:$row->{line}: stale bucket definition for $key";
	}

	if (!exists $manifest_fields{$key})
	{
		push @errors,
		  "$row->{file}:$row->{line}: bucket definition for $key has no lifecycle manifest row";
	}

	foreach my $column (qw(initializer early_adoption reset_destroy))
	{
		validate_lifecycle_action_cell($row, $column);

		foreach my $function (runtime_function_refs($row->{$column}))
		{
			if (!exists $source_functions{$function})
			{
				push @errors,
				  "$row->{file}:$row->{line}: $column references $function(), but no definition was found in the checked runtime sources";
			}
		}
	}
}

foreach my $field (@fields)
{
	my $key = field_key($field);

	push @errors, "missing bucket definition for $key"
	  unless exists $bucket_fields{$key};
}

foreach my $row (@reset_rows)
{
	my $key = field_key($row);

	if (exists $reset_fields{$key})
	{
		push @errors,
		  "$row->{file}:$row->{line}: duplicate reset definition for $key";
		next;
	}

	$reset_fields{$key} = $row;

	if (!exists $header_fields{$key})
	{
		push @errors,
		  "$row->{file}:$row->{line}: stale reset definition for $key";
	}

	if (!exists $manifest_fields{$key})
	{
		push @errors,
		  "$row->{file}:$row->{line}: reset definition for $key has no lifecycle manifest row";
	}

	foreach my $function (runtime_function_refs($row->{reset_destroy}))
	{
		if (!exists $source_functions{$function})
		{
			push @errors,
			  "$row->{file}:$row->{line}: reset_destroy references $function(), but no definition was found in the checked runtime sources";
		}
	}

	validate_lifecycle_action_cell($row, 'reset_destroy');
}

foreach my $row (@manifest_rows)
{
	next unless $row->{object} eq 'PgSession';
	next unless $row->{reset_destroy} =~ /\bPgSessionResetClosedState\b/;

	my $key = field_key($row);

	push @errors,
	  "$manifest:$row->{line}: reset_destroy names PgSessionResetClosedState(), but $key has no ordered reset definition"
	  unless exists $reset_fields{$key};
}

push @errors, require_function_calls(
	'InitializePgProcessRuntime',
	[qw(PgCarrierInitializeRuntimeObject
		PgBackendInitializeRuntimeObject
		PgSessionInitializeRuntimeObject
		PgConnectionInitializeRuntimeObject
		PgExecutionInitializeRuntimeObject
		PgBackendAdoptEarlyState
		PgSessionAdoptEarlyState
		PgConnectionAdoptEarlyState
		PgExecutionAdoptEarlyState)]);

push @errors, require_function_calls(
	'InitializePgThreadBackendRuntimeState',
	[qw(PgCarrierInitializeRuntimeObject
		PgBackendInitializeRuntimeObject
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

printf "runtime lifecycle check passed: %d fields classified, %d bucket definitions checked, %d reset definitions checked, %d owner mappings checked\n",
  scalar @fields, scalar @bucket_rows, scalar @reset_rows, scalar @owner_rows;
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
  --bucket-def FILE root-object bucket definition file to validate
  --reset-def FILE  ordered reset definition file to validate
  --owner-map FILE  symbol-level owner map path
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
	  qw(PgRuntime PgCarrier PgBackend PgSession PgConnection PgExecution);

	while (my $line = <$fh>)
	{
		if ($line =~ /^struct\s+(PgRuntime|PgCarrier|PgBackend|PgSession|PgConnection|PgExecution)\s*$/)
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

		$line =~ s/^\s*volatile\s+/ /;

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

sub read_file
{
	my ($file) = @_;

	open my $fh, '<', $file or die "could not open $file: $!";
	local $/;
	return <$fh>;
}

sub defined_functions
{
	my ($text) = @_;
	my %functions;

	while ($text =~ /^([A-Za-z_][A-Za-z0-9_]*)\s*\(/mg)
	{
		$functions{$1} = 1;
	}

	while ($text =~ /^\s*PG_RUNTIME_DEFINE_[A-Z0-9_]+\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,/mg)
	{
		$functions{$1} = 1;
	}

	return %functions;
}

sub runtime_function_refs
{
	my ($text) = @_;
	my %refs;

	while ($text =~ /\b((?:PgRuntime|PgCarrier|PgBackend|PgSession|PgConnection|PgExecution|InitializePg)[A-Za-z0-9_]*)\s*\(/g)
	{
		$refs{$1} = 1;
	}

	return sort keys %refs;
}

sub validate_lifecycle_action_cell
{
	my ($row, $column) = @_;
	my $text = $row->{$column};
	my %known_actions = map { $_ => 1 } qw(
	  PG_RUNTIME_NOOP
	  PG_RUNTIME_DELETE_MEMORY_CONTEXT
	  PG_RUNTIME_DELETE_MEMORY_CONTEXT_AND_RESET
	  PG_RUNTIME_RESET_THROUGH_INITIALIZER
	  PG_RUNTIME_DESTROY_HASH
	  PG_RUNTIME_LIST_FREE
	  PG_RUNTIME_LIST_FREE_DEEP);

	if ($text =~ /^\(void\)\s*0$/)
	{
		push @errors,
		  "$row->{file}:$row->{line}: $column uses bare (void) 0; use PG_RUNTIME_NOOP so no-op lifecycle intent is checked";
	}

	while ($text =~ /\b(PG_RUNTIME_[A-Z0-9_]+)\b/g)
	{
		my $action = $1;

		next if exists $known_actions{$action};

		push @errors,
		  "$row->{file}:$row->{line}: $column uses unknown lifecycle action $action";
	}
}

sub read_bucket_defs
{
	my (@files) = @_;
	my @rows;
	my %objects = (
		RUNTIME => 'PgRuntime',
		BACKEND => 'PgBackend',
		CARRIER => 'PgCarrier',
		SESSION => 'PgSession',
		CONNECTION => 'PgConnection',
		EXECUTION => 'PgExecution',
	);

	foreach my $file (@files)
	{
		open my $fh, '<', $file or die "could not open $file: $!";

		while (my $line = <$fh>)
		{
			chomp $line;
			next if $line =~ /^\s*$/;
			next if $line =~ /^\s*(?:\/\*|\*)/;

			if ($line !~ /^\s*PG_(RUNTIME|BACKEND|CARRIER|SESSION|CONNECTION|EXECUTION)_BUCKET\s*\((.*)\)\s*$/)
			{
				push @errors, "$file:$.: expected PG_*_BUCKET(...) row";
				next;
			}

			my $object = $objects{$1};
			my @args = split_macro_args($2);
			if (@args != 4)
			{
				push @errors,
				  "$file:$.: expected 4 bucket definition arguments, got " . scalar(@args);
				next;
			}

			push @rows,
			  {
				object => $object,
				field => $args[0],
				initializer => $args[1],
				early_adoption => $args[2],
				reset_destroy => $args[3],
				file => $file,
				line => $.,
			  };
		}
	}

	return @rows;
}

sub read_reset_defs
{
	my (@files) = @_;
	my @rows;

	foreach my $file (@files)
	{
		open my $fh, '<', $file or die "could not open $file: $!";

		while (my $line = <$fh>)
		{
			chomp $line;
			next if $line =~ /^\s*$/;
			next if $line =~ /^\s*(?:\/\*|\*)/;

			if ($line !~ /^\s*PG_SESSION_RESET_BUCKET\s*\((.*)\)\s*$/)
			{
				push @errors,
				  "$file:$.: expected PG_SESSION_RESET_BUCKET(...) row";
				next;
			}

			my @args = split_macro_args($1);
			if (@args != 2)
			{
				push @errors,
				  "$file:$.: expected 2 reset definition arguments, got " . scalar(@args);
				next;
			}

			push @rows,
			  {
				object => 'PgSession',
				field => $args[0],
				reset_destroy => $args[1],
				file => $file,
				line => $.,
			  };
		}
	}

	return @rows;
}

sub read_owner_map
{
	my ($file) = @_;

	open my $fh, '<', $file or die "could not open $file: $!";

	my $header = <$fh>;
	chomp $header if defined $header;
	if (!defined $header ||
		$header ne "legacy_symbol\troot_object\tbucket\tmember\taccessor\towner_source\tnotes")
	{
		push @errors,
		  "$file:1: expected owner-map header legacy_symbol/root_object/bucket/member/accessor/owner_source/notes";
		return ();
	}

	my @rows;
	while (my $line = <$fh>)
	{
		chomp $line;
		next if $line =~ /^\s*$/;

		my @columns = split /\t/, $line, 7;
		if (@columns != 7)
		{
			push @errors,
			  "$file:$.: expected 7 tab-separated owner-map columns, got " . scalar(@columns);
			next;
		}

		push @rows,
		  {
			legacy_symbol => $columns[0],
			object => $columns[1],
			field => $columns[2],
			member => $columns[3],
			accessor => $columns[4],
			owner_source => $columns[5],
			notes => $columns[6],
			file => $file,
			line => $.,
		  };
	}

	return @rows;
}

sub validate_owner_map
{
	my ($rows, $manifest_fields, $header_file) = @_;
	my %symbols;
	my %accessor_text_cache;

	foreach my $row (@{$rows})
	{
		my $location = "$row->{file}:$row->{line}";
		my $key = field_key($row);

		foreach my $column (qw(legacy_symbol object field member accessor owner_source notes))
		{
			push @errors, "$location: owner-map column $column must not be empty"
			  if $row->{$column} eq '';
		}

		if (exists $symbols{$row->{legacy_symbol}})
		{
			push @errors,
			  "$location: duplicate owner-map legacy symbol $row->{legacy_symbol}";
		}
		$symbols{$row->{legacy_symbol}} = 1;

		if (!exists $manifest_fields->{$key})
		{
			push @errors,
			  "$location: owner-map bucket $key has no lifecycle manifest row";
		}

		if (!-f $row->{owner_source})
		{
			push @errors,
			  "$location: owner source $row->{owner_source} does not exist";
			next;
		}

		my $text_key = "$header_file\0$row->{owner_source}";
		my $text = $accessor_text_cache{$text_key};
		if (!defined $text)
		{
			$text = read_file($header_file) . "\n" . read_file($row->{owner_source});
			$accessor_text_cache{$text_key} = $text;
		}

		if ($text !~ /\b\Q$row->{accessor}\E\b/)
		{
			push @errors,
			  "$location: accessor $row->{accessor} was not found in $header_file or $row->{owner_source}";
		}
	}
}

sub split_macro_args
{
	my ($text) = @_;
	my @args;
	my $current = '';
	my $depth = 0;

	foreach my $char (split //, $text)
	{
		if ($char eq ',' && $depth == 0)
		{
			push @args, trim($current);
			$current = '';
			next;
		}

		$current .= $char;
		$depth++ if $char eq '(' || $char eq '[' || $char eq '{';
		$depth-- if $char eq ')' || $char eq ']' || $char eq '}';
	}

	push @args, trim($current);
	return @args;
}

sub trim
{
	my ($value) = @_;

	$value =~ s/^\s+//;
	$value =~ s/\s+$//;
	return $value;
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
