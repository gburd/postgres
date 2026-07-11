#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# scan_global_lifetimes.pl
#	  Heuristic scanner for mutable global lifetime annotations.
#
# Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
#
# src/tools/global_lifetime/scan_global_lifetimes.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use Digest::SHA qw(sha1_hex);
use File::Find;
use Getopt::Long qw(GetOptions);

my %annotations = (
	PG_GLOBAL_RUNTIME => 'runtime-global',
	PG_GLOBAL_IMMUTABLE => 'immutable-singleton',
	PG_GLOBAL_DYNAMIC => 'dynamic-singleton',
	PG_GLOBAL_BACKEND => 'backend-local',
	PG_GLOBAL_SESSION => 'session-local',
	PG_GLOBAL_EXECUTION => 'execution-local',
	PG_GLOBAL_CARRIER => 'carrier-local',
	PG_GLOBAL_CONNECTION => 'connection-local',
	PG_GLOBAL_SHMEM => 'shared-memory',
);

my $baseline_file;
my $write_baseline;
my $report_file;
my $show_classified = 0;
my $all_unclassified = 0;
my $enforce_local_runtime_boundary = 0;
my $help = 0;

GetOptions(
	'baseline=s' => \$baseline_file,
	'write-baseline=s' => \$write_baseline,
	'report=s' => \$report_file,
	'show-classified' => \$show_classified,
	'all-unclassified' => \$all_unclassified,
	'enforce-local-runtime-boundary' => \$enforce_local_runtime_boundary,
	'help' => \$help,
) or usage(2);

usage(0) if $help;

my @roots = @ARGV ? @ARGV : qw(src/backend src/include);
my @files;

foreach my $root (@roots)
{
	next unless -e $root;

	if (-f $root)
	{
		push @files, $root if wanted_file($root);
		next;
	}

	find(
		{
			wanted => sub {
				return unless -f $_;
				return unless wanted_file($File::Find::name);
				push @files, $File::Find::name;
			},
			no_chdir => 1,
		},
		$root);
}

@files = sort @files;

my @records;
foreach my $file (@files)
{
	push @records, scan_file($file);
}

my @classified = grep { $_->{owner} ne 'unclassified' } @records;
my @unclassified = grep { $_->{owner} eq 'unclassified' } @records;
my %owner_counts;

foreach my $record (@records)
{
	$owner_counts{$record->{owner}}++;
}

my %baseline;
if (defined $baseline_file)
{
	%baseline = read_baseline($baseline_file);
}

if (defined $write_baseline)
{
	write_baseline($write_baseline, \@unclassified);
}

if (defined $report_file)
{
	write_report($report_file, \@records, $show_classified);
}

print_summary(\%owner_counts, scalar @records);

if ($all_unclassified)
{
	print_records('unclassified mutable globals', \@unclassified);
}

if ($show_classified)
{
	print_records('classified mutable globals', \@classified);
}

if (defined $baseline_file)
{
	my @new = grep { !exists $baseline{ $_->{key} } } @unclassified;

	if (@new)
	{
		print "\nnew unclassified mutable globals:\n";
		print_record($_) foreach @new;
		exit 1;
	}

	print "\nnew unclassified mutable globals: 0\n";
}

if ($enforce_local_runtime_boundary)
{
	my @violations = grep { local_runtime_boundary_violation($_) } @classified;

	if (@violations)
	{
		print "\nlocal runtime boundary violations:\n";
		print_record($_) foreach @violations;
		exit 1;
	}

	print "local runtime boundary violations: 0\n";
}

exit 0;

sub usage
{
	my ($status) = @_;

	print <<'USAGE';
Usage: perl src/tools/global_lifetime/scan_global_lifetimes.pl [OPTIONS] [PATH...]

Options:
  --baseline FILE        Fail if unclassified globals are not present in FILE
  --write-baseline FILE  Write current unclassified globals to FILE
  --report FILE          Write a TSV report
  --show-classified      Print/write classified declarations too
  --all-unclassified     Print all current unclassified declarations
  --enforce-local-runtime-boundary
                         Fail if backend/session/execution/connection/carrier
                         local globals appear outside the runtime bridge
  --help                 Show this help
USAGE

	exit $status;
}

sub wanted_file
{
	my ($file) = @_;

	return 0 if $file =~ m{/(tmp_check|tmp_check_iso|output_iso|results|expected)/};
	return 0 if $file =~ m{/(po)/};
	# Generated Bison compatibility code uses K&R-style helper definitions that
	# this heuristic scanner mistakes for top-level globals.  The grammar source
	# remains annotated directly in bootparse.y.
	return 0 if $file =~ m{^src/backend/bootstrap/bootparse\.c$};
	return 0 if $file =~ m{^src/backend/parser/gram\.c$};
	return 0 if $file =~ m{^src/backend/replication/(?:repl|syncrep)_gram\.c$};
	return 0 if $file =~ m{^src/backend/utils/adt/jsonpath_gram\.c$};
	# Generated Flex scanner tables are immutable parser metadata.  Scanning the
	# generated C directly is noisy and does not classify hand-written globals.
	return 0 if $file =~ m{^src/backend/parser/scan\.c$};
	return 0 if $file =~ m{^src/backend/utils/adt/jsonpath_scan\.c$};
	# This file is deliberately included inside checksum_impl.h function bodies.
	# Scanning it as a standalone header mistakes local checksum scratch
	# variables for top-level globals.
	return 0 if $file =~ m{^src/include/storage/checksum_block_internal\.h$};
	# Generated node switch fragments are included inside functions.  They are
	# not declaration units, and scanning them directly mistakes case-body
	# assignments for globals.
	return 0 if $file =~ m{^src/backend/nodes/.*funcs\.switch\.c$};
	return $file =~ /\.(?:c|h)$/;
}

sub scan_file
{
	my ($file) = @_;
	open my $fh, '<', $file or die "could not open $file: $!";

	my @records;
	my $decl = '';
	my $decl_line = 0;
	my $brace_depth = 0;
	my $skip_block_depth = 0;
	my $in_comment = 0;
	my $in_preprocessor = 0;

	while (my $line = <$fh>)
	{
		my $lineno = $.;

		if ($in_preprocessor)
		{
			strip_comments($line, \$in_comment);
			$in_preprocessor = ($line =~ /\\\s*$/);
			next;
		}
		if ($line =~ /^\s*#/)
		{
			strip_comments($line, \$in_comment);
			$in_preprocessor = ($line =~ /\\\s*$/);
			next;
		}

		my $clean = strip_comments($line, \$in_comment);

		next if $clean =~ /^\s*$/ && $decl eq '';

		if ($skip_block_depth > 0)
		{
			$skip_block_depth += brace_delta($clean);
			$skip_block_depth = 0 if $skip_block_depth < 0;
			next;
		}

		my $starts_decl = ($decl eq '' && $brace_depth == 0);

		if ($starts_decl)
		{
			next if $clean =~ /^\s*(?:else\b|return\b|case\b|default:|\})/;
			$decl_line = $lineno;
		}

		if ($brace_depth == 0
			&& ($decl . $clean) =~ /{/
			&& should_skip_top_level_block($decl . $clean))
		{
			$skip_block_depth = brace_delta($decl . $clean);
			$skip_block_depth = 0 if $skip_block_depth < 0;
			$decl = '';
			$decl_line = 0;
			next;
		}

		$decl .= $clean;

		$brace_depth += brace_delta($clean);
		$brace_depth = 0 if $brace_depth < 0;

		if ($brace_depth == 0 && $decl =~ /;/)
		{
			foreach my $statement (split_top_level_statements($decl))
			{
				my $record = classify_declaration($file, $decl_line, $statement);
				push @records, $record if defined $record;
			}
			$decl = '';
			$decl_line = 0;
		}
	}

	close $fh;
	return @records;
}

sub should_skip_top_level_block
{
	my ($text) = @_;
	my ($prefix) = split /{/, $text, 2;

	return 1 if $prefix =~ /^\s*(?:typedef\s+)?(?:struct|union|enum)\b/;
	return 0 if $prefix =~ /=/;
	return $prefix =~ /\)\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*)*$/;
}

sub brace_delta
{
	my ($text) = @_;

	$text =~ s/"(?:\\.|[^"\\])*"//g;
	$text =~ s/'(?:\\.|[^'\\])*'//g;
	return ($text =~ tr/{//) - ($text =~ tr/}//);
}

sub strip_comments
{
	my ($line, $in_comment_ref) = @_;
	my $out = '';

	while (length $line)
	{
		if ($$in_comment_ref)
		{
			if ($line =~ s/^.*?\*\///)
			{
				$$in_comment_ref = 0;
			}
			else
			{
				return $out . "\n";
			}
		}
		elsif ($line =~ s/^(.*?)\/\*//)
		{
			$out .= $1;
			$$in_comment_ref = 1;
		}
		else
		{
			$out .= $line;
			last;
		}
	}

	$out =~ s{//.*$}{};
	return $out;
}

sub split_top_level_statements
{
	my ($text) = @_;
	my @statements;
	my $current = '';
	my $depth = 0;

	foreach my $char (split //, $text)
	{
		$current .= $char;
		$depth++ if $char eq '{' || $char eq '(' || $char eq '[';
		$depth-- if $char eq '}' || $char eq ')' || $char eq ']';
		$depth = 0 if $depth < 0;

		if ($char eq ';' && $depth == 0)
		{
			push @statements, $current;
			$current = '';
		}
	}

	return @statements;
}

sub classify_declaration
{
	my ($file, $line, $decl) = @_;
	my $normalized = normalize_declaration($decl);

	return undef if $normalized eq '';
	return undef if should_skip_declaration($normalized);

	my $owner = 'unclassified';
	foreach my $annotation (sort keys %annotations)
	{
		if ($normalized =~ /\b\Q$annotation\E\b/)
		{
			$owner = $annotations{$annotation};
			last;
		}
	}

	return undef if $owner eq 'unclassified' && is_immutable_const_object($normalized);

	my @names = extract_names($normalized);
	return undef unless @names;

	my $key = "$file\t" . sha1_hex($normalized);

	return {
		owner => $owner,
		file => $file,
		line => $line,
		names => join(',', @names),
		declaration => $normalized,
		key => $key,
	};
}

sub normalize_declaration
{
	my ($decl) = @_;

	$decl =~ s/\s+/ /g;
	$decl =~ s/^\s+//;
	$decl =~ s/\s+$//;
	return $decl;
}

sub should_skip_declaration
{
	my ($decl) = @_;

	return 1 if $decl =~ /\btypedef\b/;
	return 1 if $decl =~ /^extern\s+int\s+\w+_yydebug\s*;/;
	return 1 if $decl =~ /^static\s+inline\b/;
	return 1 if $decl =~ /^extern\s+(?:PGDLLIMPORT\s+|PGDLLEXPORT\s+)?(?:pg_noreturn\s+)?(?:void|bool|int|char|const|struct|enum|[A-Za-z_][A-Za-z0-9_]*)\b.*\)\s*;/ && $decl !~ /=/;
	return 1 if $decl =~ /^(?:static\s+)?[A-Za-z_][A-Za-z0-9_\s\*]*\s+[A-Za-z_][A-Za-z0-9_]*\s*\(/ && $decl =~ /\)\s*;/ && $decl !~ /=/;
	return 1 if $decl =~ /\)\s*;/ && $decl !~ /=/ && $decl !~ /\(\s*\*/;
	return 1 if $decl =~ /^[A-Z_][A-Z0-9_]*\s*(?:\([^;]*\))?\s*;$/;
	return 1 if $decl =~ /^\w+\s*\([^;]*\)\s*;/;
	return 1 if $decl =~ /^(?:struct|union|enum)\s+\w+\s*;/;
	return 1 if $decl =~ /^(?:struct|union|enum)\s+\w+\s*{/;
	return 1 if $decl =~ /^{/;
	return 1 if $decl =~ /^}/;
	return 1 if $decl =~ /^(?:[A-Za-z_][A-Za-z0-9_]*\s*\([^;]*\)\s*)+[A-Za-z_][A-Za-z0-9_]*\s*;$/;

	return 0;
}

sub is_immutable_const_object
{
	my ($decl) = @_;
	my $signature = $decl;

	$signature =~ s/=.*$//;

	return 0 unless $signature =~ /\bconst\b/;
	return 0 if $signature =~ /\*/ && $signature !~ /\*\s*const\b/;
	return 1;
}

sub extract_names
{
	my ($decl) = @_;
	my $work = $decl;

	$work =~ s/\bPG_GLOBAL_[A-Z_]+\b//g;
	$work =~ s/\b(?:extern|static|PGDLLIMPORT|PGDLLEXPORT|volatile|const|register)\b//g;
	$work =~ s/__attribute__\s*\(\([^)]*\)\)//g;
	$work =~ s/=\s*(?:\{[^;]*\}|[^,;]*)(?=,|;)//g;

	my @names;
	while ($work =~ /([A-Za-z_][A-Za-z0-9_]*)\s*(?:\[[^\]]*\])?\s*(?:=|,|;)/g)
	{
		my $name = $1;
		next if is_keyword($name);
		push @names, $name;
	}

	return @names;
}

sub is_keyword
{
	my ($word) = @_;
	my %keywords = map { $_ => 1 } qw(
	  auto break case char const continue default do double else enum extern
	  float for goto if inline int long register restrict return short signed
	  sizeof static struct switch typedef union unsigned void volatile while
	  bool true false NULL
	);

	return $keywords{$word};
}

sub read_baseline
{
	my ($file) = @_;
	my %baseline;

	open my $fh, '<', $file or die "could not open baseline $file: $!";
	while (my $line = <$fh>)
	{
		chomp $line;
		next if $line =~ /^\s*#/ || $line =~ /^\s*$/;

		my ($path, $hash) = split /\t/, $line, 3;
		next unless defined $path && defined $hash;
		$baseline{"$path\t$hash"} = 1;
	}
	close $fh;

	return %baseline;
}

sub write_baseline
{
	my ($file, $records) = @_;

	open my $fh, '>', $file or die "could not write baseline $file: $!";
	print $fh "# file\tsha1\tnames\tdeclaration\n";
	foreach my $record (sort record_sort @$records)
	{
		my (undef, $hash) = split /\t/, $record->{key}, 2;
		print $fh join("\t",
					   $record->{file},
					   $hash,
					   $record->{names},
					   display_declaration($record->{declaration})), "\n";
	}
	close $fh;
}

sub write_report
{
	my ($file, $records, $include_classified) = @_;

	open my $fh, '>', $file or die "could not write report $file: $!";
	print $fh "# owner\tfile\tline\tnames\tdeclaration\n";
	foreach my $record (sort record_sort @$records)
	{
		next if !$include_classified && $record->{owner} ne 'unclassified';
		print $fh join("\t",
					   $record->{owner},
					   $record->{file},
					   $record->{line},
					   $record->{names},
					   $record->{declaration}), "\n";
	}
	close $fh;
}

sub print_summary
{
	my ($counts, $total) = @_;

	print "global lifetime scan summary\n";
	print "  declarations scanned: $total\n";

	foreach my $owner (sort keys %$counts)
	{
		printf "  %-20s %d\n", $owner . ':', $counts->{$owner};
	}
}

sub local_runtime_boundary_violation
{
	my ($record) = @_;
	my %local_owners = map { $_ => 1 } qw(
	  backend-local carrier-local connection-local execution-local session-local
	);
	my $file = $record->{file};

	return 0 unless $local_owners{ $record->{owner} };

	# The remaining core local globals should be either the runtime bridge's
	# process objects/current pointers/early fallback storage, or explicitly
	# documented platform/test shims that cannot use the runtime object path.
	return 0 if $file eq 'src/backend/utils/init/backend_runtime.c';
	return 0 if $file eq 'src/include/utils/backend_runtime.h';
	return 0 if $file eq 'src/include/utils/backend_runtime_current.h';
	return 0
	  if $record->{owner} eq 'backend-local'
	  && $file eq 'src/backend/utils/init/backend_runtime_backend.c';
	return 0
	  if $record->{owner} eq 'connection-local'
	  && $file eq 'src/backend/libpq/backend_runtime_connection.c';
	return 0
	  if $record->{owner} eq 'execution-local'
	  && $file eq 'src/backend/utils/init/backend_runtime_execution.c';
	return 0
	  if $record->{owner} eq 'session-local'
	  && $file eq 'src/backend/utils/init/backend_runtime_session.c';

	# Standalone spinlock test wait-event storage.
	return 0
	  if $record->{owner} eq 'backend-local'
	  && ($file eq 'src/backend/storage/lmgr/s_lock.c'
		  || $file eq 'src/include/utils/wait_event.h');

	# Windows carrier signal/timer shims.
	return 0
	  if $record->{owner} eq 'carrier-local'
	  && ($file eq 'src/backend/port/win32/signal.c'
		  || $file eq 'src/backend/port/win32/timer.c'
		  || $file eq 'src/include/port/win32_port.h');

	# Threaded backend exit handoff for memory retained after logical cleanup.
	return 0
	  if $record->{owner} eq 'carrier-local'
	  && $file eq 'src/backend/storage/ipc/ipc.c';

	return 1;
}

sub print_records
{
	my ($title, $records) = @_;

	print "\n$title:\n";
	print_record($_) foreach sort record_sort @$records;
}

sub print_record
{
	my ($record) = @_;

	printf "  %s:%d [%s] %s\n",
	  $record->{file},
	  $record->{line},
	  $record->{names},
	  $record->{declaration};
}

sub display_declaration
{
	my ($decl) = @_;

	$decl =~ s/\t/ /g;
	return length($decl) > 240 ? substr($decl, 0, 237) . '...' : $decl;
}

sub record_sort
{
	return $a->{file} cmp $b->{file}
	  || $a->{line} <=> $b->{line}
	  || $a->{names} cmp $b->{names};
}
