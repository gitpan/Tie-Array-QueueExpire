#!/usr/bin/perl

use strict;
use Data::Dumper;

#use lib "./lib";
use Tie::Array::QueueExpire;
my $max = shift;
my $exp = shift;
my $t   = tie( my @myarray, "Tie::Array::QueueExpire", '/tmp/db_test.bdb', 0666 );
my @test;

if ( $max )
{
    for ( 1 .. $max )
    {
        my $rnd = int rand 1000;
        push @myarray, $rnd;
        push @test,    $rnd;
    }
}
my $s = scalar( @myarray );
print "size = <$s>\n";

print "\n original tie array  and real array comparaison \n";
print_diff( \@myarray, \@test );

print "\n executing splice \n";
my @tmp   = splice @myarray, 5 ,3;
my @tmp_t = splice @test,5,3;

print "\n tie array  and real array comparaison after splice \n";
print_diff( \@myarray, \@test );

print "\n tmp and tmp_t comparaison \n";
print_diff( \@tmp, \@tmp_t );

#print Dumper(\@myarray);

#print Dumper(\@myarray);
#print Dumper(\@tmp);
#print Dumper(\@test);
#print Dumper(\@tmp_t);


for ( 0 .. $#myarray )
{
    print "data in $_ =" . $myarray[$_] . "\n";
}


my $data = shift @myarray;
my $data_t  = shift @test;
print "-+-+-+-+--+shifted data=$data\n";



print_diff( \@myarray, \@test );

my $data = pop @myarray;
my $data_t  = pop @test;
print "-+-+-+-+--+poped data=$data\n";

print_diff( \@myarray, \@test );

print "is lement 6 exist ?<" . exists( $myarray[6] ) . "> val is<" . $myarray[6] . ">\n";

print "is lement 1000 exist ?<" . exists( $myarray[1000] ) . ">\n";

if ( $exp )
{
    print "expired=";
    my @EXP =  $t->EXPIRE( $exp ) ;
    print "number of elements oldest than the expiration time = ".scalar @EXP . "\t deleted=";
    my @EXPD =  $t->EXPIRE( $exp, 1 ) ;
    print "number of elements deleted by expiration = ".scalar @EXP . "\n";
}

print "After expiration " .Dumper( \@myarray );
$t->OPTIMIZE();

sub print_diff
{
    my $array1 = shift;
    my $array2 = shift;
    my @ARRAY1 = @{ $array1 };
    my @ARRAY2 = @{ $array2 };
    my $ind    = 0;
    foreach ( @ARRAY1 )
    {
        print "$ind\t<" . $ARRAY1[$ind] . ">\t<" . $ARRAY2[$ind] . ">\n";
	$ind++;
    }

}
