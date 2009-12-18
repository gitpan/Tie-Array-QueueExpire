#!/usr/bin/perl
use strict;
use feature qw( say );
use Data::Dumper;

use lib './lib';
use Tie::Array::QueueExpire;

my $t = tie( my @myarray, "Tie::Array::QueueExpire", '/tmp/db_test.bdb' );

for ( 1 .. 10 )
{
 say  "t=".time.'  '.  int (($t->PUSH( $_ . ' ' . time, 10 ))/1000);
    sleep 1;
}

say "time=" . time;
my $ex = $t->EXPIRE( 7 );
say Dumper( $ex );
sleep 10;
say "time=" . time;
my $ex = $t->EXPIRE( 7 );
say Dumper( $ex );

say      "toto=".time.'  '.  int (($t->PUSH( 'toto' . ' ' . time, 2 ))/1000);
for ( 11 .. 20 )
{
   say   "t=".time.'  '.  int (( $t->PUSH( $_ . ' ' . time ))/1000);
    sleep 1;
}
say "time=" . time ;
$ex = $t->EXPIRE( 7 );
say Dumper( $ex );
sleep 5;

say Dumper( @myarray );
$ex =  $t->EXPIRE(20,1);
say Dumper($ex);

say "time=" . time;
$ex = $t->EXPIRE( 7 );
say Dumper( $ex );
say  "t=".time.'  '. int (( $t->PUSH( 'tata' . ' ' . time, -14 ))/1000);
for ( 1 .. 10 )
{
 say  "t=".time.'  '.  int (($t->PUSH( $_ . ' ' . time, 10 ))/1000);
    sleep 1;
}

say Dumper( @myarray );

my $a =$t->FETCH(6);
my @b = $t->FETCH(6);
my @c=$myarray[6];

say Dumper( $a );
say Dumper( \@b );
say Dumper( \@c );

my $l = $t->LAST;
say $l;
say Dumper($t->LAST);
say scalar ($t->FIRST);
