#!/usr/bin/perl
#----------------------------------------------------------------------
#
# Generate guc_tables.c from guc_parameters.dat.
#
# Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/backend/utils/misc/gen_guc_tables.pl
#
#----------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../catalog";
use Catalog;

die "Usage: $0 INPUT_FILE OUTPUT_FILE\n" unless @ARGV == 2;
my ($input_fname, $output_fname) = @ARGV;

my $parse = Catalog::ParseData($input_fname);

open my $ofh, '>', $output_fname or die;

print_boilerplate($ofh, $output_fname, 'GUC tables');
print_table($ofh);
print_variable_pointer_rebind($ofh);
print_threaded_session_guc_rebinds($ofh);

close $ofh;


# Adds double quotes and escapes as necessary for C strings.
sub dquote
{
	my ($s) = @_;

	return q{"} . $s =~ s/"/\\"/gr . q{"};
}

sub validate_guc_entry
{
	my ($entry) = @_;

	my @required_common =
	  qw(name type context group short_desc variable boot_val);

	my %required_by_type = (
		int => [qw(min max)],
		real => [qw(min max)],
		enum => [qw(options)],
		bool => [],      # no extra required fields
		string => [],    # no extra required fields
	);

	# All fields recognized by the generator.  "line_number" is injected
	# by Catalog::ParseData and is not a user-facing field.
	my %valid_fields = map { $_ => 1 } (
		@required_common,
		qw(long_desc flags ifdef min max options
		  check_hook assign_hook show_hook
		  threaded_accessor line_number));

	for my $f (sort keys %$entry)
	{
		unless ($valid_fields{$f})
		{
			die sprintf(
				qq{%s:%d: error: entry "%s" has unrecognized field "%s"\n},
				$input_fname, $entry->{line_number},
				$entry->{name} // '<unknown>', $f);
		}
	}

	for my $f (@required_common)
	{
		unless (defined $entry->{$f})
		{
			die sprintf(
				qq{%s:%d: error: entry "%s" is missing required field "%s"\n},
				$input_fname, $entry->{line_number},
				$entry->{name} // '<unknown>', $f);
		}
	}

	unless (exists $required_by_type{ $entry->{type} })
	{
		die sprintf(
			qq{%s:%d: error: entry "%s" has unrecognized GUC type "%s"\n},
			$input_fname, $entry->{line_number},
			$entry->{name}, $entry->{type} // '<unknown>');
	}

	for my $f (@{ $required_by_type{ $entry->{type} } })
	{
		unless (defined $entry->{$f})
		{
			die sprintf(
				qq{%s:%d: error: entry "%s" of type "%s" is missing required field "%s"\n},
				$input_fname, $entry->{line_number}, $entry->{name},
				$entry->{type}, $f);
		}
	}
}

# Print GUC table.
sub print_table
{
	my ($ofh) = @_;
	my $prev_name = undef;

	print $ofh "\n\n";
	print $ofh "PG_GLOBAL_IMMUTABLE struct config_generic ConfigureNames[] =\n";
	print $ofh "{\n";

	foreach my $entry (@{$parse})
	{
		validate_guc_entry($entry);

		if (defined($prev_name) && lc($prev_name) eq lc($entry->{name}))
		{
			die sprintf(qq{%s:%d: error: duplicate entry "%s"\n},
				$input_fname, $entry->{line_number}, $entry->{name});
		}
		if (defined($prev_name) && lc($prev_name) gt lc($entry->{name}))
		{
			die sprintf(
				qq{%s:%d: error: entries are not in alphabetical order: "%s", "%s"\n},
				$input_fname, $entry->{line_number},
				$prev_name, $entry->{name});
		}

		print $ofh "#ifdef $entry->{ifdef}\n" if $entry->{ifdef};
		print $ofh "\t{\n";
		printf $ofh "\t\t.name = %s,\n", dquote($entry->{name});
		printf $ofh "\t\t.context = %s,\n", $entry->{context};
		printf $ofh "\t\t.group = %s,\n", $entry->{group};
		printf $ofh
		  "\t\t/* translator: GUC parameter \"%s\" short description */\n",
		  $entry->{name};
		printf $ofh "\t\t.short_desc = gettext_noop(%s),\n",
		  dquote($entry->{short_desc});

		if ($entry->{long_desc})
		{
			printf $ofh
			  "\t\t/* translator: GUC parameter \"%s\" long description */\n",
			  $entry->{name};
			printf $ofh "\t\t.long_desc = gettext_noop(%s),\n",
			  dquote($entry->{long_desc});
		}
		printf $ofh "\t\t.flags = %s,\n", $entry->{flags} if $entry->{flags};
		printf $ofh "\t\t.vartype = %s,\n", ('PGC_' . uc($entry->{type}));
		printf $ofh "\t\t._%s = {\n", $entry->{type};
		print $ofh "\t\t\t.variable = NULL,\n";
		printf $ofh "\t\t\t.boot_val = %s,\n", $entry->{boot_val};
		printf $ofh "\t\t\t.min = %s,\n", $entry->{min}
		  if $entry->{type} eq 'int' || $entry->{type} eq 'real';
		printf $ofh "\t\t\t.max = %s,\n", $entry->{max}
		  if $entry->{type} eq 'int' || $entry->{type} eq 'real';
		printf $ofh "\t\t\t.options = %s,\n", $entry->{options}
		  if $entry->{type} eq 'enum';
		printf $ofh "\t\t\t.check_hook = %s,\n", $entry->{check_hook}
		  if $entry->{check_hook};
		printf $ofh "\t\t\t.assign_hook = %s,\n", $entry->{assign_hook}
		  if $entry->{assign_hook};
		printf $ofh "\t\t\t.show_hook = %s,\n", $entry->{show_hook}
		  if $entry->{show_hook};
		print $ofh "\t\t},\n";
		print $ofh "\t},\n";
		print $ofh "#endif\n" if $entry->{ifdef};
		print $ofh "\n";

		$prev_name = $entry->{name};
	}

	print $ofh "\t/* End-of-list marker */\n";
	print $ofh "\t{0}\n";
	print $ofh "};\n";

	return;
}

sub print_variable_pointer_rebind
{
	my ($ofh) = @_;

	print $ofh "\n\n";
	print $ofh "void\n";
	print $ofh "InitializeGUCVariablePointers(struct config_generic *variables)\n";
	print $ofh "{\n";
	print $ofh "\tint\t\t\ti = 0;\n\n";

	foreach my $entry (@{$parse})
	{
		print $ofh "#ifdef $entry->{ifdef}\n" if $entry->{ifdef};
		printf $ofh "\tAssert(strcmp(variables[i].name, %s) == 0);\n",
		  dquote($entry->{name});
		printf $ofh "\tvariables[i]._%s.variable = &%s;\n",
		  $entry->{type}, $entry->{variable};
		print $ofh "\ti++;\n";
		print $ofh "#endif\n" if $entry->{ifdef};
		print $ofh "\n";
	}

	print $ofh "\tAssert(variables[i].name == NULL);\n";
	print $ofh "}\n";

	return;
}

sub threaded_session_guc_macro
{
	my ($type) = @_;

	my %macro_by_type = (
		bool => 'PG_SESSION_GUC_BOOL',
		int => 'PG_SESSION_GUC_INT',
		real => 'PG_SESSION_GUC_REAL',
		string => 'PG_SESSION_GUC_STRING',
		enum => 'PG_SESSION_GUC_ENUM',
	);

	return $macro_by_type{$type}
	  // die "unexpected GUC type \"$type\" while generating threaded rebinds";
}

sub print_threaded_session_guc_rebinds
{
	my ($ofh) = @_;
	my $count = 0;

	print $ofh "\n\n";
	print $ofh "#define PG_SESSION_GUC_BOOL(name, accessor) \\\n";
	print $ofh "\t{name, PGC_BOOL, {.bool_ref = accessor}}\n";
	print $ofh "#define PG_SESSION_GUC_INT(name, accessor) \\\n";
	print $ofh "\t{name, PGC_INT, {.int_ref = accessor}}\n";
	print $ofh "#define PG_SESSION_GUC_REAL(name, accessor) \\\n";
	print $ofh "\t{name, PGC_REAL, {.real_ref = accessor}}\n";
	print $ofh "#define PG_SESSION_GUC_STRING(name, accessor) \\\n";
	print $ofh "\t{name, PGC_STRING, {.string_ref = accessor}}\n";
	print $ofh "#define PG_SESSION_GUC_ENUM(name, accessor) \\\n";
	print $ofh "\t{name, PGC_ENUM, {.enum_ref = accessor}}\n";
	print $ofh "\n";
	print $ofh "PG_GLOBAL_IMMUTABLE const ThreadedSessionGUCRebind ThreadedSessionGUCRebinds[] =\n";
	print $ofh "{\n";

	foreach my $entry (@{$parse})
	{
		next unless $entry->{threaded_accessor};

		print $ofh "#ifdef $entry->{ifdef}\n" if $entry->{ifdef};
		printf $ofh "\t%s(%s, %s),\n",
		  threaded_session_guc_macro($entry->{type}),
		  dquote($entry->{name}),
		  $entry->{threaded_accessor};
		print $ofh "#endif\n" if $entry->{ifdef};
		$count++;
	}

	print $ofh "};\n\n";
	printf $ofh "PG_GLOBAL_IMMUTABLE const int NumThreadedSessionGUCRebinds = lengthof(ThreadedSessionGUCRebinds);\n";
	print $ofh "\n";
	print $ofh "#undef PG_SESSION_GUC_BOOL\n";
	print $ofh "#undef PG_SESSION_GUC_INT\n";
	print $ofh "#undef PG_SESSION_GUC_REAL\n";
	print $ofh "#undef PG_SESSION_GUC_STRING\n";
	print $ofh "#undef PG_SESSION_GUC_ENUM\n";

	die "no threaded session GUC rebinds generated" if $count == 0;

	return;
}

sub print_boilerplate
{
	my ($fh, $fname, $descr) = @_;
	printf $fh <<EOM, $fname, $descr;
/*-------------------------------------------------------------------------
 *
 * %s
 *    %s
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *  ******************************
 *  *** DO NOT EDIT THIS FILE! ***
 *  ******************************
 *
 *  It has been GENERATED by src/backend/utils/misc/gen_guc_tables.pl
 *
 *-------------------------------------------------------------------------
 */
EOM

	return;
}
