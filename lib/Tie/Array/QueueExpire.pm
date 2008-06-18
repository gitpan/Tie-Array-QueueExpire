package Tie::Array::QueueExpire;
###########################################################
# Tie::Array::QueueExpire package
# Gnu GPL2 license
#
# $Id: QueueExpire.pm 49 2008-06-18 07:54:31Z fabrice $
# $Revision: 49 $
#
# Fabrice Dulaunoy <fabrice@dulaunoy.com>
###########################################################
# ChangeLog:
#
###########################################################

=head1  Tie::Array::QueueExpire - Introduction

  Tie::Array::QueueExpire - Tie an ARRAY over a TokyCabinet Btree DB ( see http://tokyocabinet.sourceforge.net )
  $Revision: 49 $

=head1 SYNOPSIS

  use Tie::Array::QueueExpire;
  my $t = tie( my @myarray, "Tie::Array::QueueExpire", '/tmp/db_test.bdb' );
  push @myarray, int rand 1000;
  
  # normal ARRAY function
  my $data = shift @myarray;
  my $data = pop @myarray;
  print "this elem exists\n"  if (exists( $myarray[6]));
  print "size = ". scalar( @myarray )."\n";
  
  
  my $exp = 1207840028;
  # Get the expired elements
  my @EXP = @{$t->EXPIRE($exp)};
  # Delete the expired elements
  my @EXP = @{$t->EXPIRE($exp,1)};
  
=head1 DESCRIPTION

  Tie an ARRAY over a TokyCabinet Btree DB and allow to get or deleted expired data;
  
  This module require Time::hiRes, TokyoCabinet database and perl module.
  
  The normal ARRAY function present are
  
  push
  pop
  shift
  exits
  scalar
  clear
  unshift  (but put data 1 second before the first entry)

  The following function is not completely iplemented.
  
  splice
  
  The following function are not implemented.
  
  extend
  store
  STORESIZE

  The following function are specific of this module.
  
  LAST
  FIRST
  EXPIRE
  OPTIMIZE
  
 
=cut

use 5.008008;
use strict;
use warnings;
use Tie::Array;
use Time::HiRes qw{ time };
require Exporter;

use Carp;
use TokyoCabinet;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

$VERSION = sprintf "0.%02d", '$Revision: 49 $ ' =~ /(\d+)/;

our @ISA = qw( Exporter Tie::StdArray );

=head1 Basic ARRAY functions
	
I< >
	
=head2 tie
	
	Tie an array over a DB
	my $t = tie( my @myarray, "Tie::Array::QueueExpire", '/tmp/db_test.bdb' );
	The fist parameter if the TokyoCabinet file used (or created)
	A second optional parameter allow to set the permission in octal of the DB created
	The default permisscion is 0600 (-rw-------) 
      
=cut

sub TIEARRAY
{
    my $class = $_[0];
    my %data;
    $data{ _file } = $_[1];
    $data{ _mode } = $_[2] || 0600;
    my $bdb = TokyoCabinet::BDB->new();
    if ( !$bdb->open( $data{ _file }, $bdb->OWRITER | $bdb->OCREAT ) )
    {
        my $ecode = $bdb->ecode();
        croak( "open error: %s\n", $bdb->errmsg( $ecode ) );
    }
    my $mode= $data{ _mode };
    chmod $mode  ,$data{ _file }; 
    $data{ _bdb } = $bdb;
    bless \%data, $class;
    return \%data;
}

=head2 FETCH
	
	Retrieve a specific key from the array
	my $data = $myarray[6];
      
=cut


sub FETCH
{
    my $self = shift;
    my $key  = shift;
    my $bdb  = $self->{ _bdb };
    return undef unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );
    my $status = $cur->jump($key);
    return $cur->val();
}

=head2 FETCHTIME
	
	Retrieve a specific key from the array with the TIME tag
	my ($data, $ticks) = $t->FETCHTIME(6);
      
=cut

sub FETCHTIME
{
    my $self = shift;
    my $key  = shift;
    my $bdb  = $self->{ _bdb };
    return undef unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );
    my $status = $cur->jump($key);
    return $cur->val(), $cur->key();
}

=head2 FETCHSIZE
	
	Get the size of the array
	my $data = scalar(@myarray);
      
=cut

sub FETCHSIZE
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    return $bdb->rnum();
}

=head2 PUSH
	
	Add an element at the end of the array
	push @myarray , 45646;
      
=cut

sub PUSH
{
    my $self  = shift;
    my $value = shift;
    my $bdb   = $self->{ _bdb };
    $bdb->put( time, $value );
}

=head2 EXISTS
	
	Test if en element in the array exist
	print "element exists\n" if (exits $myarray[5]);
      
=cut

sub EXISTS
{
    my $self = shift;
    my $key  = shift;
    my $bdb  = $self->{ _bdb };
    return 0 unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );
    return $cur->jump($key);
}

=head2 POP
	
	Extract the latest element from the array (the youngest)
	my $data = pop @myarray;
      
=cut

sub POP
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    my $data = $bdb->get( $self->LAST() );
    $bdb->out( $self->LAST() );
    return $data;
}

=head2 SHIFT
	
	Extract the first element from the array  (the oldest)
	my $data = pop @myarray;
      
=cut

sub SHIFT
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    my $data = $bdb->get( $self->FIRST() );
    $bdb->out( $self->FIRST() );
    return $data;
}

=head2 UNSHIFT
	
	Add an element in the front of the array
	unshift @myarray , 45646;
	UNSHIFT data 1 second before the first item
	
=cut

sub UNSHIFT {
    my $self  = shift;
    my $value = shift;
    my $bdb   = $self->{ _bdb };
    my $first = $bdb->get( $self->FIRST() );
    $bdb->put( $first-1, $value );
 }

=head2 CLEAR
	
	Delete all element in the array
	$t->CLEAR;
      
=cut

sub CLEAR
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    return $bdb->vanish();
}

=head2 DESTROY
	
	Normal destructor call when untied the array
	Normaly never called by user
	
=cut

sub DESTROY
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    $bdb->close();
}

=head1 Specific functions from this module

I< >

=head2 SPLICE
	
	SPLICE don't allow a list replacement 
	because the insert order is made by time.
	my @tmp   = splice @myarray, 5 ,3;
	
=cut

sub SPLICE
{
    my $self   = shift;
    my $offset = shift || 0;
    my $length = shift || 0;
    my $bdb    = $self->{ _bdb };
    my @all;
    unless ( $offset + $length )
    {
        $self->CLEAR;
        return @all;
    }
    return \@all unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );

    if ( $offset > 0 )
    {
        $cur->first();
        for ( 1 .. $offset )
        {
            $cur->next();
        }

        if ( $length > 0 )
        {
            for ( 1 .. $length )
            {
                push @all, $cur->val();
                $cur->out();
            }
        }
        else
        {
            my $max = $self->FETCHSIZE();
            for ( $offset + 1 .. $max + $length )
            {
                push @all, $cur->val();
                $cur->out();
            }
        }
    }
    else
    {
        $cur->last();
        for ( $offset + 2 .. 0 )
        {
            $cur->prev();
        }
        if ( $length > 0 )
        {
            for ( 1 .. $length )
            {
                push @all, $cur->val();
                $cur->out();
            }
        }
        else
        {
            my $max = $self->FETCHSIZE();
            my $ind = 0;
            for ( $offset + 1 .. $max + $length )
            {
                push @all, $cur->val() if ( ( $cur->key() ) && ( $ind < abs $length ) );
                $cur->out();
                $ind++;
            }
        }
    }
    return @all;
}

=head2 LAST
	
	Get the latest element in the array (oldest)
	my $data =$t->LAST;
      
=cut

sub LAST
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    return undef unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );
    $cur->last();
    return $cur->key();
}

=head2 FIRST
	
	Get the first element in the array (youngest)
	my $data =$t->LAST;
      
=cut

sub FIRST
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    return undef unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );
    $cur->first();
    return $cur->key();
}

=head2 EXPIRE
	
	Get the elements expired in the array.
	my @ALL = $t->EXPIRE( 1207840028) ;
	return a refernce to an array with all the expired value.
	
	If a second parameter is provided and not null, the data are also deleted from the array.
	my @ALL = $t->EXPIRE( 1207840028 , 1 ) ;
	return a refernce to an array with all the expired value.
	
=cut

sub EXPIRE
{
    my $self   = shift;
    my $time   = shift;
    my $to_del = shift;
    my $bdb    = $self->{ _bdb };
    my @all;
    return \@all unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );
    $cur->first();

    while ( $cur->key() <= $time )
    {
        push @all, $cur->val();
        $cur->out() if ( $to_del );
        $cur->next() unless ( $to_del );
    }
    return @all;
}

=head2 OPTIMIZE
	
	Function to compact the DB (after a lot of delete )
	$t->OPTIMIZE;
=cut

sub OPTIMIZE
{
    my $self = shift;
    my ( $lmemb, $nmemb, $bnum, $apow, $fpow, $opts ) = ( @_ );
    my $bdb = $self->{ _bdb };
    $bdb->optimize( $lmemb, $nmemb, $bnum, $apow, $fpow, $opts );
    chmod $self->{ _mode }, $self->{ _file };
}

=head1 Functions not Implemented

I< >


=head2 EXTEND
	
	Not implemented because not signifiant for a expiration queue
	
=cut

sub EXTEND { carp "no EXTEND function"; }

=head2 STORE
	
	Not implemented because not signifiant for a expiration queue
	
=cut

sub STORE { carp "no STORE function"; }

=head2 STORESIZE
	
	Not implemented because not signifiant for a expiration queue
	
=cut

sub STORESIZE { carp "no STORESIZE function"; }

1;
__END__

		

=head1 AUTHOR

	Fabrice Dulaunoy <fabrice_at_dulaunoy_dot_com> 
	

=head1 SEE ALSO

	- Data::Queue::Persistent from Mischa Spiegelmock, <mspiegelmock_at_gmail_dot_com>
        - TokyoCabinet from Mikio Hirabayashi <mikio_at_users_dot_sourceforge_dot_net>


=head1 TODO

        - make test
        - implementation of EXTEND to allow clear of array with @myarray = ();
	- implementation of STORESIZE to allow clear of array with $#myarray = -1;
	
=head1 LICENSE

	Under the GNU GPL2

	This program is free software; you can redistribute it and/or modify it 
	under the terms of the GNU General Public 
	License as published by the Free Software Foundation; either version 2 
	of the License, or (at your option) any later version.

	This program is distributed in the hope that it will be useful, 
	but WITHOUT ANY WARRANTY;  without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
	See the GNU General Public License for more details.

	You should have received a copy of the GNU General Public License 
	along with this program; if not, write to the 
	Free Software Foundation, Inc., 59 Temple Place, 
	Suite 330, Boston, MA 02111-1307 USA

	Tie::Array::QueueExpire  Copyright (C) 2004 2005 2006 2007 DULAUNOY Fabrice  
	Tie::Array::QueueExpire comes with ABSOLUTELY NO WARRANTY; 
	for details See: L<http://www.gnu.org/licenses/gpl.html> 
	This is free software, and you are welcome to redistribute 
	it under certain conditions;
   
   
=cut
