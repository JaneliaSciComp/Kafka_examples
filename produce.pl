#!/usr/bin/perl

use strict;
use warnings;
use Kafka::Connection;
use Kafka::Producer;
use POSIX qw(strftime);
use Scalar::Util qw(blessed);
use Try::Tiny;

my ($connection,$producer);

sub logError
{
  my $msg = shift;
  die($msg);
}


sub publish
{
  return unless ($producer);
  my($message) = shift;
  try {
    my $t = time;
    my $stamp = strftime "%Y-%m-%d %H:%M:%S", localtime $t;
    $stamp .= sprintf ".%03d", ($t-int($t))*1000;
    my $response = $producer->send('test',0,$message,$stamp,undef,time*1000);
  }
  catch {
    my $error = $_;
    if (blessed($error) && $error->isa('Kafka::Exception')) {
      &logError('Error: ('.$error->code.') '.$error->message);
    }
    else {
      &logError($error);
    }
  };
}


sub initialize
{
  try {
    $connection = Kafka::Connection->new(host => 'kafka.int.janelia.org',
                                         timeout => 5);
    $producer = Kafka::Producer->new(Connection => $connection)
      if ($connection);
  }
  catch {
    my $error = $_;
    if (blessed($error) && $error->isa('Kafka::Exception')) {
      &logError('Error: ('.$error->code.') '.$error->message);
    }
    else {
      &logError($error);
    }
  };
  &logError("Couldn't connect to Kafka") unless ($producer);
}

&initialize();
for (1..10) {
  &publish("Periodic message $_ from Perl producer");
  sleep(2);
}
