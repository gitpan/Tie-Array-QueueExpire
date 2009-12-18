package Tie::Array::QueueExpire;
###########################################################
# Tie::Array::QueueExpire package
# Gnu GPL2 license
#
# $Id: QueueExpire.pm 55 2008-09-24 11:22:05Z fabrice $
#
# Fabrice Dulaunoy <fabrice@dulaunoy.com>
###########################################################
# ChangeLog:
#
###########################################################

=head1  Tie::Array::QueueExpire - Introduction

  Tie::Array::QueueExpire - Tie an ARRAY over a TokyCabinet Btree DB ( see http://tokyocabinet.sourceforge.net )
  Revision: 1.01

=head1 SYNOPSIS

  use Tie::Array::QueueExpire;
  my $t = tie( my @myarray, "Tie::Array::QueueExpire", '/tmp/db_test.bdb' );
  push @myarray, int rand 1000;
  
  # normal ARRAY function
  my $data = shift @myarray;
  my $data = pop @myarray;
  print "this elem exists\n"  if (exists( $myarray[6]));
  print "size = ". scalar( @myarray )."\n";
  
  # using the PUSH with an extra parameter to put the new element in futur
  # also return the key of the inserted value
  for ( 1 .. 10 )
  {
    say  "t=".time.'  '.  int (($t->PUSH( $_ . ' ' . time, 10 ))/1000);
    sleep 1;
  }
  sleep 10;  
  # Get the expired elements ( 7 seconds before now )
  my $ex = $t->EXPIRE( 7 );
 
  # Get the expired elements
  my @EXP = @{$t->EXPIRE($exp)};
  # Get and delete the expired elements ( 20 seconds before now )
  $ex =  $t->EXPIRE(20,1);
  my @EXP = @{$t->EXPIRE($exp,1)};
  
  # fetch element
  # in scalar context return the value 
  # in array context return in first element the key and in second, the value
  my $a =$t->FETCH(6);
  my @b = $t->FETCH(6);
  # the normal array fetch is always in scalar mode
  my @c=$myarray[6];
  say Dumper( $a );
  say Dumper( \@b );
  say Dumper( \@c );
  
=head1 DESCRIPTION

  Tie an ARRAY over a TokyCabinet Btree DB and allow to get or deleted expired data;
  
  This module require Time::HiRes, TokyoCabinet (database and perl module.)
  The insertion is ms unique ( 0.001 seconds )
  
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
  PUSH
  FETCH
  
 
=cut

use 5.008008;
use strict;
use warnings;
use Tie::Array;
use Time::HiRes qw{ gettimeofday };
require Exporter;

use Carp;
use TokyoCabinet;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

$VERSION = 1.01;

our @ISA = qw( Exporter Tie::StdArray );

=head1 Basic ARRAY functions
	
I< >
	
=head2 tie
	
	Tie an array over a TokyoCabinet DB
	my $t = tie( my @myarray, "Tie::Array::QueueExpire", '/tmp/db_test.bdb' );
	The fist parameter if the TokyoCabinet file used (or created)
        Four optional parameter are allowed
	In place two, a flag to serialize the data in the DB
	In place three, an octal MASK allow to set the permission of the DB created
		The default permission is 0600 (-rw-------) 
        In place four a parameter to delete the DB file if present and corrupted
       		The default value is 0 (don't delete the file)
	In place five a parameter to exit on error when opening the DB file
       		The default value is 0 (don't exit, only report error)
      
=cut

sub TIEARRAY
{
    my $class = $_[0];
    my %data;
    $data{ _file }            = $_[1];
    $data{ _serialize }       = $_[2] || 0;
    $data{ _mode }            = $_[3] || 0600;
    $data{ _delete_on_error } = $_[4] || 0;
    $data{ _exit_on_error }   = $_[5] || 0;
    my $bdb = TokyoCabinet::BDB->new();
    $bdb->setcmpfunc( $bdb->CMPDECIMAL );
    my $serialiser;

    if ( $data{ _serialize } )
    {
        use Data::Serializer;
        $serialiser = Data::Serializer->new( compress => 0 );
        $data{ _serialize } = $serialiser;
    }
    if ( !$bdb->open( $data{ _file }, $bdb->OWRITER | $bdb->OCREAT | $bdb->ONOLCK | $bdb->OLCKNB ) )
    {
        my $ecode = $bdb->ecode();
        if ( $data{ _delete_on_error } )
        {
            unlink $data{ _file };
            if ( !$bdb->open( $data{ _file }, $bdb->OWRITER | $bdb->OCREAT | $bdb->ONOLCK | $bdb->OLCKNB ) )
            {
                my $ecode = $bdb->ecode();
                if ( $data{ _exit_on_error } )
                {
                    croak( "open error: " . $bdb->errmsg( $ecode . "\n" ) );
                }
                else
                {
                    carp( "open error: " . $bdb->errmsg( $ecode . "\n" ) );
                }
            }
        }
        else
        {
            if ( $data{ _exit_on_error } )
            {
                croak( "open error after delete: " . $bdb->errmsg( $ecode . "\n" ) );
            }
            else
            {
                carp( "open error after delete: " . $bdb->errmsg( $ecode . "\n" ) );
            }
        }
    }

    my $mode = $data{ _mode };
    chmod $mode, $data{ _file };
    $data{ _bdb } = $bdb;
    bless \%data, $class;
    return \%data;
}

=head2 FETCH
	
	Retrieve a specific key from the array
	my $data = $myarray[6];
	or
	my $data = $t->FETCH(6);
	or 
	my @data = $t->FETCH(6);
	where 
	  $data[0] = insertion key
	and 
	  $data[1] = value 
      
=cut

sub FETCH
{
    my $self = shift;
    my $key  = shift;
    my $bdb  = $self->{ _bdb };
    return undef unless ( $bdb->rnum() );
    my $cur    = TokyoCabinet::BDBCUR->new( $bdb );
    my $status = $cur->first();
    for ( 1 .. $key )
    {
        $status = $cur->next();
    }
    my $val = $cur->val();
    $val = $self->__deserialize__( $val ) if ( $self->{ _serialize } );
    if ( wantarray )
    {
        return $cur->key(), $val;
    }
    else
    {
        return $val;
    }
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
	or 
	$t->PUSH( 45646 );
	it is also possible to add an elemnt with a offset expiration 
	$t->PUSH( 45646 , 10 );
	add element in the array to be expired in 10 seconds
	if the offset is negative, add the expiration in past
      
=cut

sub PUSH
{
    my $self  = shift;
    my $value = shift;
    my $time  = shift || 0;
    my $bdb   = $self->{ _bdb };
    $value = $self->__serialize__( $value ) if ( $self->{ _serialize } );
    my ( $sec, $usec ) = gettimeofday;
    $sec += $time if ( $time != 0 );
    $usec = int( $usec / 1000 );
    my $k = sprintf( "%010d%03d", $sec, $usec );
    $bdb->put( $k, $value );
    $bdb->sync();
    return $k;
}

=head2 EXISTS
	
	Test if en element in the array exist
	print "element exists\n" if (exits $myarray[5]);
	return the insertion key
      
=cut

sub EXISTS
{
    my $self = shift;
    my $key  = shift;
    my $bdb  = $self->{ _bdb };
    return 0 unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );
    return $cur->jump( $key );
}

=head2 POP
	
	Extract the latest element from the array (the youngest)
	my $data = pop @myarray;
      	or
	my $data = $t->POP();
	or 
	my @data = $t->POP();
	where 
	  $data[0] = insertion key
	and 
	  $data[1] = value 
=cut

sub POP
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    my $key  = ( $self->LAST() )[0];
    my $val  = $bdb->get( $key );
    $bdb->out( $key );
    $bdb->sync();
    $val = $self->__deserialize__( $val ) if ( $self->{ _serialize } );
    if ( wantarray )
    {
        return $key, $val;
    }
    else
    {
        return $val;
    }
}

=head2 SHIFT
	
	Extract the first element from the array  (the oldest)
	my $data = shift @myarray;
	or
	my $data = $t->SHIFT();
	or 
	my @data = $t->SHIFT();
       where 
	  $data[0] = insertion key
	and 
	  $data[1] = value 
=cut

sub SHIFT
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    my $key  = ( $self->FIRST() )[0];
    my $val  = $bdb->get( $key );
    $bdb->out( $key );
    $bdb->sync();
    $val = $self->__deserialize__( $val ) if ( $self->{ _serialize } );
    if ( wantarray )
    {
        return $key, $val;
    }
    else
    {
        return $val;
    }
}

=head2 UNSHIFT
	
	Add an element in the front of the array
	unshift @myarray , 45646;
	UNSHIFT data 1 mili-second before the first item
	
=cut

sub UNSHIFT
{
    my $self  = shift;
    my $value = shift;
    my $bdb   = $self->{ _bdb };
    my $first = $bdb->get( $self->FIRST() );
    $value = $self->__serialize__( $value ) if ( $self->{ _serialize } );
    $bdb->put( $first - 1, $value );
    $bdb->sync();
    return $first - 1;
}

=head2 CLEAR
	
	Delete all element in the array
	$t->CLEAR;
      
=cut

sub CLEAR
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    $bdb->sync();
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
                my $val = $cur->val();
                $val = $self->__deserialize__( $val ) if ( $self->{ _serialize } );
                push @all, $val;
                $cur->out();
            }
        }
        else
        {
            my $max = $self->FETCHSIZE();
            for ( $offset + 1 .. $max + $length )
            {
                my $val = $cur->val();
                $val = $self->__deserialize__( $val ) if ( $self->{ _serialize } );
                push @all, $val;
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
                my $val = $cur->val();
                $val = $self->__deserialize__( $val ) if ( $self->{ _serialize } );
                push @all, $val;
                $cur->out();
            }
        }
        else
        {
            my $max = $self->FETCHSIZE();
            my $ind = 0;
            for ( $offset + 1 .. $max + $length )
            {
                if ( ( $cur->key() ) && ( $ind < abs $length ) )
                {
                    my $val = $cur->val();
                    $val = $self->__deserialize__( $val ) if ( $self->{ _serialize } );
                    push @all, $val;
                }
                $cur->out();
                $ind++;
            }
        }
    }
    $bdb->sync();
    return @all;
}

=head2 LAST
	
	Get the latest element in the array (oldest)
	my $data = $t->LAST;
        or
        my @data = $t->LAST;
	where 
	  $data[0] = insertion key
	and 
	  $data[1] = value 
=cut

sub LAST
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    return undef unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );
    $cur->last();
    my $val = $cur->val();
    $val = $self->__deserialize__( $val ) if ( $self->{ _serialize } );
    if ( wantarray )
    {
        return $cur->key(), $val;
    }
    else
    {
        return $val;
    }
}

=head2 FIRST
	
	Get the first element in the array (youngest)
	my $data =$t->FIRST;
        or
        my @data = $t->FIRST;
	where 
	  $data[0] = insertion key
	and 
	  $data[1] = value 
=cut

sub FIRST
{
    my $self = shift;
    my $bdb  = $self->{ _bdb };
    return undef unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );
    $cur->first();
    my $val = $cur->val();
    $val = $self->__deserialize__( $val ) if ( $self->{ _serialize } );
    if ( wantarray )
    {
        return $cur->key(), $val;
    }
    else
    {
        return $val;
    }
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
    my $self = shift;
    my $time = shift;
    $time = time - $time;
    $time *= 1000;
    my $to_del = shift || 0;
    my $bdb    = $self->{ _bdb };
    my @all    = ();

    return \@all unless ( $bdb->rnum() );
    my $cur = TokyoCabinet::BDBCUR->new( $bdb );
    $cur->first();
    while ( $cur->key() <= $time )
    {
        my $val = $cur->val();
        $val = $self->__deserialize__( $val ) if ( $self->{ _serialize } );
        push @all, $val;
        if ( $to_del )
        {
            last unless ( $cur->out() );
        }
        else
        {
            last unless ( $cur->next() );
        }
    }
    $bdb->sync();
    return \@all;
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

sub __serialize__
{
    my $self       = shift;
    my $val        = shift;
    my $serializer = $self->{ _serialize };
    return $serializer->serialize( $val ) if $val;
    return $val;
}

sub __deserialize__
{
    my $self       = shift;
    my $val        = shift;
    my $serializer = $self->{ _serialize };
    return $serializer->deserialize( $val ) if $val;
    return $val;
}
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

	Tie::Array::QueueExpire  Copyright (C) 2004 2005 2006 2007 2008 2009 2010 DULAUNOY Fabrice  
	Tie::Array::QueueExpire comes with ABSOLUTELY NO WARRANTY; 
	for details See: L<http://www.gnu.org/licenses/gpl.html> 
	This is free software, and you are welcome to redistribute 
	it under certain conditions;
   
   
=cut
