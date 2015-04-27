#!/usr/bin/perl

# DevSearch

use utf8;

use strict;
use warnings;

use Encode;
use File::Basename;

my $arg_count = $#ARGV + 1;

sub usage {
    print "USAGE: splitter.pl input_file_path output_directory_path split_line_size\n";
    print "Example: ./splitter.pl megablobs_java/part-00001 miniblobs_java 100M\n";
}

if ($arg_count != 3) {
    usage();
    die "Invalid arguments";
}

# http://stackoverflow.com/a/2557660 by rjh
sub convert_human_size_to_bytes {
    my $size = shift;
    my @suffixes = ('', qw(k m g));
    for my $index (0..$#suffixes) {
        my $suffix = $suffixes[$index];
        if ( $size =~ /^([\d.]+)$suffix\z/i ) {
            return int($1 * (1024 ** $index));
        }
    }
    # No match
    die "Didn't understand human-readable file size '$size'";  # or croak
}

my $inputFilePath = $ARGV[0];
my $inputFileName = basename($inputFilePath);
my $outputFolderName = $ARGV[1];
$outputFolderName =~ s/\/$//;

my $blobSize = convert_human_size_to_bytes($ARGV[2]);

open (my $inputFile, '<', $inputFilePath) or die "Can't open $inputFilePath: $!";

my $blobIndex = 0;
my $bytesCount = 0;
my $blobFile;

while (my $line = <$inputFile>) {
    if ($line =~ /^(\d+):\.\.\/data\/crawld\/[\w\.\-]+\/[\w\.\-]+\/[\w\.\-]+\//) {
        my $nextBlobSize = $1;        
        
        # If the current blob is full
	my $isInitializing = ($blobIndex == 0 && $bytesCount == 0);
        if ((($bytesCount + $nextBlobSize) >= $blobSize)
             or $isInitializing) {
            if (!$isInitializing) {
                $blobIndex++;
                $bytesCount = 0;
            }

            # Close current blob and open a new one
            defined $blobFile and close ($blobFile);
            my $outputFilename = $outputFolderName."/".$inputFileName."_$blobIndex";
            open ($blobFile, '>', $outputFilename) or die "Can't open $outputFilename for output: $!";
        }
    }
    my $lineSize = length($line);
    $bytesCount += $lineSize;
    print $blobFile $line;
}

defined $blobFile and close ($blobFile);
close ($inputFile);
