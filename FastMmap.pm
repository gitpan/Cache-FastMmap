package Cache::FastMmap;

use Data::Dumper;

=head1 NAME

Cache::FastMmap - Uses an mmap'ed file to act as a shared memory interprocess cache

=head1 SYNOPSIS

  use Cache::FastMmap;

  # Uses vaguely sane defaults
  $Cache = Cache::FastMmap->new();

  # $Value must be a reference...
  $Cache->set($Key, $Value);
  $Value = $Cache->get($Key);

  $Cache = Cache::FastMmap->new(raw_values => 1);

  # $Value can't be a reference...
  $Cache->set($Key, $Value);
  $Value = $Cache->get($Key);

=head1 ABSTRACT

A shared memory cache through an mmap'ed file. It's core is written
in C for performance. It uses fcntl locking to ensure multiple
processes can safely access the cache at the same time. It uses
a basic LRU algorithm to keep the most used entries in the cache.

=head1 DESCRIPTION

In multi-process environments (eg mod_perl, forking daemons, etc),
it's common to want to cache information, but have that cache
shared between processes. Many solutions already exist, and may
suit your situation better:

=over 4

=item *

L<MLDBM::Sync> - acts as a database, data is not automatically
expired, slow

=item *

L<IPC::MM> - hash implementation is broken, data is not automatically
expired, slow

=item *

L<Cache::FileCache> - lots of features, slow

=item *

L<Cache::SharedMemoryCache> - lots of features, VERY slow. Uses
IPC::ShareLite which freeze/thaws ALL data at each read/write

=item *

L<DBI> - use your favourite RDBMS. can perform well, need a
DB server running. very global. socket connection latency

=item *

L<Cache::Mmap> - similar to this module, in pure perl. slows down
with larger pages

=item *

L<BerkeleyDB> - very fast (data ends up mostly in shared memory
cache) but acts as a database overall, so data is not automatically
expired

=back

In the case I was working on, I needed:

=over 4

=item *

Automatic expiry and space management

=item *

Very fast access to lots of small items

=item *

The ability to fetch/store many items in one go

=back

Which is why I developed this module. It tries to be quite
efficient through a number of means:

=over 4

=item *

Core code is written in C for performance

=item *

It uses multiple pages within a file, and uses Fcntl to only lock
a page at a time to reduce contention when multiple processes access
the cache.

=item *

It uses a dual level hashing system (hash to find page, then hash
within each page to find a slot) to make most C<get()> calls O(1) and
fast

=item *

On each C<set()>, if there are slots and page space available, only
the slot has to be updated and the data written at the end of the used
data space. If either runs out, a re-organisation of the page is
performed to create new slots/space which is done in an efficient way

=back

The class also supports read-through, and write-back or write-through
callbacks to access the real data if it's not in the cache, meaning that
code like this:

  my $Value = $Cache->get($Key);
  if (!defined $Value) {
    $Value = $RealDataSource->get($Key);
    $Cache->set($Key, $Value)
  }

Isn't required, you instead specify in the constructor:

  Cache::FastMmap->new(
    ...
    context => $RealDataSourceHandle,
    read_cb => sub { $_[0]->get($_[1]) },
    write_cb => sub { $_[0]->set($_[1], $_[2]) },
  );

And then:

  my $Value = $Cache->get($Key);

  $Cache->set($Key, $NewValue);

Will just work and will be read/written to the underlying data source as
needed automatically.

=head1 PERFORMANCE

If you're storing relatively large and complex structures into
the cache, then you're limited by the speed of the Storable module.
If you're storing simple structures, or raw data, then
Cache::FastMmap has noticeable performance improvements.

See L<http://cpan.robm.fastmail.fm/cache_perf.html> for some
comparisons to other modules.

=head1 COMPATIABILITY

Cache::FastMmap uses mmap to map a file as the shared cache space,
and fcntl to do page locking. This means it should work on most
UNIX like operating systems, but will not work on Windows or
Win32 like environments.

=head1 MEMORY SIZE

Because Cache::FastMmap mmap's a shared file into your processes memory
space, this can make each process look quite large, even though it's just
mmap'd memory that's shared between all processes that use the cache,
and may even be swapped out if the cache is getting low usage.

However, the OS will think your process is quite large, which might
mean you hit some BSD::Resource or 'ulimits' you set previously that you
thought were sane, but aren't anymore, so be aware.

=head1 USAGE

Because the cache uses shared memory through an mmap'd file, you have
to make sure each process connects up to the file. There's probably
two main ways to do this:

=over 4

=item *

Create the cache in the parent process, and then when it forks, each
child will inherit the same file descriptor, mmap'ed memory, etc and
just work.

=item *

Explicitly connect up in each forked child to the share file

=back

The first way is usually the easiest. If you're using the cache in a
Net::Server based module, you'll want to open the cache in the
C<pre_loop_hook>, because that's executed before the fork, but after
the process ownership has changed and any chroot has been done.

In mod_perl, just open the cache at the global level in the appropriate
module, which is executed as the server is starting and before it
starts forking children, but you'll probably want to chmod or chown
the file to the permissions of the apache process.

=head1 METHODS

=over 4

=cut

# Modules/Export/XSLoader {{{
use 5.006;
use strict;
use warnings;
use bytes;
use Cache::FastMmap::CImpl;

require Exporter;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use Cache::FastMmap ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
	
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
	
);

our $VERSION = '1.14';

use constant FC_ISDIRTY => 1;
# }}}

=item I<new(%Opts)>

Create a new Cache::FastMmap object.

Basic global parameters are:

=over 4

=item * B<share_file>

File to mmap for sharing of data (default: /tmp/sharefile)

=item * B<init_file>

Clear any existing values and re-initialise file. Useful to do in a
parent that forks off children to ensure that file is empty at the start
(default: 0)

B<Note:> This is quite important to do in the parent to ensure a
consistent file structure. The shared file is not perfectly transaction
safe, and so if a child is killed at the wrong instant, it might leave
the the cache file in an inconsistent state.

=item * B<raw_values>

Store values as raw binary data rather than using Storable to free/thaw
data structures (default: 0)

=item * B<expire_time>

Maximum time to hold values in the cache in seconds. A value of 0
means does no explicit expiry time, and values are expired only based
on LRU usage. Can be expressed as 1m, 1h, 1d for minutes/hours/days
respectively. (default: 0)

=back

You may specify the cache size as:

=over 4

=item * B<cache_size>

Size of cache. Can be expresses as 1k, 1m for kilobytes or megabytes
respectively. Automatically guesses page size/page count values.

=back

Or specify explicit page size/page count values. If none of these are
specified, the values page_size = 64k and num_pages = 89 are used.

=over 4

=item * B<page_size>

Size of each page. Must be a power of 2 between 4k and 1024k. If not,
is rounded to the nearest value.

=item * B<num_pages>

Number of pages. Should be a prime number for best hashing

=back

The cache allows the use of callbacks for reading/writing data to an
underlying data store.

=over 4

=item * B<context>

Opaque reference passed as the first parameter to any callback function
if specified

=item * B<read_cb>

Callback to read data from the underlying data store.  Called as:

  $read_cb->($context, $Key)
  
Should return the value to use. This value will be saved in the cache
for future retrievals. Return undef if there is no value for the
given key

=item * B<write_cb>

Callback to write data to the underlying data store.
Called as:

  $write_cb->($context, $Key, $Value, $ExpiryTime)
  
In 'write_through' mode, it's always called as soon as a I<set(...)>
is called on the Cache::FastMmap class. In 'write_back' mode, it's
called when a value is expunged from the cache if it's been changed
by a I<set(...)> rather than read from the underlying store with the
I<read_cb> above.

Note: Expired items do result in the I<write_cb> being
called if 'write_back' caching is enabled and the item has been
changed. You can check the $ExpiryTime against C<time()> if you only
want to write back values which aren't expired.

Also remember that I<write_cb> may be called in a different process
to the one that placed the data in the cache in the first place

=item * B<delete_cb>

Callback to delete data from the underlying data store.  Called as:

  $delete_cb->($context, $Key)

Called as soon as I<remove(...)> is called on the Cache::FastMmap class

=item * B<cache_not_found>

If set to true, then if the I<read_cb> is called and it returns
undef to say nothing was found, then that information is stored
in the cache, so that next time a I<get(...)> is called on that
key, undef is returned immediately rather than again calling
the I<read_cb>

=item * B<write_action>

Either 'write_back' or 'write_through'. (default: write_through)

=item * B<empty_on_exit>

When you have 'write_back' mode enabled, then
you really want to make sure all values from the cache are expunged
when your program exits so any changes are written back. This is a
bit tricky, because we don't know if you're in a child, so you
must ensure that the parent process either explicitly calls
I<empty()> or that this flag is set to true when the parent connects
to the cache, and false in all the children

=back

=cut
sub new {
  my $Proto = shift;
  my $Class = ref($Proto) || $Proto;
  my %Args = @_;

  my $Self = {};
  bless ($Self, $Class);

  # Work out cache file and whether to init
  my $share_file = $Self->{share_file}
    = $Args{share_file} || '/tmp/sharefile';
  my $init_file = $Args{init_file} || 0;
  my $test_file = $Args{test_file} || 0;

  # Storing raw/storable values?
  my $raw_values = $Self->{raw_values} = int($Args{raw_values} || 0);

  # Need storable module if not using raw values
  if (!$raw_values) {
    eval "use Storable qw(freeze thaw); 1;"
      || die "Could not load Storable module: $@";
  }

  # Work out expiry time in seconds
  my $expire_time = $Args{expire_time} || 0;
  my %Times = (m => 60, h => 60*60, d => 24*60*60);
  $expire_time *= $Times{$1} if $expire_time =~ s/([mhd])$//i;
  $Self->{expire_time} = $expire_time = int($expire_time);

  # Function rounds to the nearest power of 2
  sub RoundPow2 { return int(2 ** int(log($_[0])/log(2)) + 0.1); }

  # Work out cache size
  my ($cache_size, $num_pages, $page_size);

  my %Sizes = (k => 1024, m => 1024*1024);
  if ($cache_size = $Args{cache_size}) {
    $cache_size *= $Sizes{$1} if $cache_size =~ s/([km])$//i;

    if ($num_pages = $Args{num_pages}) {
      $page_size = RoundPow2($cache_size / $num_pages);
      $page_size = 4096 if $page_size < 4096;

    } else {
      $page_size = $Args{page_size} || 65536;
      $page_size *= $Sizes{$1} if $page_size =~ s/([km])$//i;
      $page_size = 4096 if $page_size < 4096;

      # Increase num_pages till we exceed 
      $num_pages = 89;
      if ($num_pages * $page_size <= $cache_size) {
        while ($num_pages * $page_size <= $cache_size) {
          $num_pages = $num_pages * 2 + 1;
        }
      } else {
        while ($num_pages * $page_size > $cache_size) {
          $num_pages = int(($num_pages-1) / 2);
        }
        $num_pages = $num_pages * 2 + 1;
      }

    }

  } else {
    ($num_pages, $page_size) = @Args{qw(num_pages page_size)};
    $num_pages ||= 89;
    $page_size ||= 65536;
    $page_size *= $Sizes{$1} if $page_size =~ s/([km])$//i;
    $page_size = RoundPow2($page_size);
  }

  $cache_size = $num_pages * $page_size;
  @$Self{qw(cache_size num_pages page_size)}
    = ($cache_size, $num_pages, $page_size);

  # Number of slots to start in each page
  my $start_slots = int($Args{start_slots} || 0) || 89;

  # Save read through/write back/write through details
  my $write_back = ($Args{write_action} || 'write_through') eq 'write_back';
  @$Self{qw(context read_cb write_cb delete_cb)}
    = @Args{qw(context read_cb write_cb delete_cb)};
  @$Self{qw(empty_on_exit cache_not_found write_back)}
    = (@Args{qw(empty_on_exit cache_not_found)}, $write_back);

  # Initialise C cache code
  my $Cache = Cache::FastMmap::CImpl::fc_new();

  # We bless the returned scalar ref into the same namespace,
  #  and store it in our own hash ref. We have to be sure
  #  that we only call C functions on this scalar ref, and
  #  only call PERL functions the hash ref we return
  bless ($Cache, 'Cache::FastMmap::CImpl');

  $Self->{Cache} = $Cache;

  # Setup cache parameters
  $Cache->fc_set_param('init_file', $init_file);
  $Cache->fc_set_param('test_file', $test_file);
  $Cache->fc_set_param('page_size', $page_size);
  $Cache->fc_set_param('num_pages', $num_pages);
  $Cache->fc_set_param('expire_time', $expire_time);
  $Cache->fc_set_param('share_file', $share_file);
  $Cache->fc_set_param('start_slots', $start_slots);

  # And initialise it
  $Cache->fc_init();

  # All done, return PERL hash ref as class
  return $Self;
}

=item I<get($Key, [ \%Options ])>

Search cache for given Key. Returns undef if not found. If
I<read_cb> specified and not found, calls the callback to try
and find the value for the key, and if found (or 'cache_not_found'
is set), stores it into the cache and returns the found value.

I<%Options> is optional, and is used by get_and_set() to control
the locking behaviour. For now, you should probably ignore it
unless you read the code to understand how it works

=cut
sub get {
  my ($Self, $Cache) = ($_[0], $_[0]->{Cache});

  # Hash value, lock page, read result
  my ($HashPage, $HashSlot) = $Cache->fc_hash($_[1]);
  $Cache->fc_lock($HashPage);
  my ($Val, $Flags, $Found) = $Cache->fc_read($HashSlot, $_[1]);

  # Value not found, check underlying data store
  if (!$Found && (my $read_cb = $Self->{read_cb})) {

    # Callback to read from underlying data store
    $Val = eval { $read_cb->($Self->{context}, $_[1]); };

    # If we found it, or want to cache not-found, store back into our cache
    if (defined $Val || $Self->{cache_not_found}) {

      # Are we doing writeback's? If so, need to mark as dirty in cache
      my $write_back = $Self->{write_back};

      # If not using raw values, use freeze() to turn data 
      $Val = freeze(\$Val) if !$Self->{raw_values};

      # Get key/value len (we've got 'use bytes'), and do expunge check to
      #  create space if needed
      my $KVLen = length($_[1]) + length($Val);
      $Self->_expunge_page(2, 1, $KVLen);

      $Cache->fc_write($HashSlot, $_[1], $Val, 0);
    }
  }

  # Unlock page and return any found value
  # Unlock is done only if we're not in the middle of a get_set() operation.
  $Cache->fc_unlock() unless $_[2] && $_[2]->{skip_unlock};

  # If not using raw values, use thaw() to turn data back into object
  if (!$Self->{raw_values}) {
    $Val = ${thaw($Val)} if defined $Val;
  }

  return $Val;
}

=item I<set($Key, $Value, [ \%Options ])>

Store specified key/value pair into cache

I<%Options> is optional, and is used by get_and_set() to control
the locking behaviour. For now, you should probably ignore it
unless you read the code to understand how it works

=cut
sub set {
  my ($Self, $Cache) = ($_[0], $_[0]->{Cache});

  # Hash value, lock page
  my ($HashPage, $HashSlot) = $Cache->fc_hash($_[1]);
  $Cache->fc_lock($HashPage) unless $_[3] && $_[3]->{skip_lock};

  # Are we doing writeback's? If so, need to mark as dirty in cache
  my $write_back = $Self->{write_back};

  # If not using raw values, use freeze() to turn data 
  my $Val = $Self->{raw_values} ? $_[2] : freeze(\$_[2]);

  # Get key/value len (we've got 'use bytes'), and do expunge check to
  #  create space if needed
  my $KVLen = length($_[1]) + length($Val);
  $Self->_expunge_page(2, 1, $KVLen);

  # Now store into cache
  my $DidStore = $Cache->fc_write($HashSlot, $_[1], $Val, $write_back ? FC_ISDIRTY : 0);

  # Unlock page
  $Cache->fc_unlock();

  # If we're doing write-through, or write-back and didn't get into cache,
  #  write back to the underlying store
  if ((!$write_back || !$DidStore) && (my $write_cb = $Self->{write_cb})) {
    eval { $write_cb->($Self->{context}, $_[1], $_[2]); };
  }

  return $DidStore;
}

=item I<get_and_set($Key, $Sub)>

Atomically retrieve and set the value of a Key.

The page is locked while retrieving the $Key and is unlocked only after
the value is set, thus guaranteeing the value does not change betwen
the get and set operations.

$Sub is a reference to a subroutine that is called to calculate the
new value to store. $Sub gets $Key and the current value
as parameters, and
should return the new value to set in the cache for the given $Key.

For example, to atomically increment a value in the cache, you
can just use:

  $Cache->get_and_set($Key, sub { return ++$_[1]; });

The return value from this function is the new value stored back
into the cache.

Notes:

=over 4

=item *

Do not perform any get/set operations from the callback sub, as these
operations lock the page and you may end up with a dead lock!

=item *

Make sure your sub does not die/throw an exception, otherwise the
unlocking code will be skipped. You can protect yourself by
wrapping everything in your sub in an C<eval { }>

=back

=cut
sub get_and_set {
  my ($Self, $Cache) = ($_[0], $_[0]->{Cache});

  my $Value = $Self->get($_[1], { skip_unlock => 1 });
  $Value = $_[2]->($_[1], $Value);
  $Self->set($_[1], $Value, { skip_lock => 1 });

  return $Value;
}

=item I<remove($Key)>

Delete the given key from the cache

=cut
sub remove {
  my ($Self, $Cache) = ($_[0], $_[0]->{Cache});

  # Hash value, lock page, read result
  my ($HashPage, $HashSlot) = $Cache->fc_hash($_[1]);
  $Cache->fc_lock($HashPage);
  my ($DidDel, $Flags) = $Cache->fc_delete($HashSlot, $_[1]);
  $Cache->fc_unlock();

  # If we deleted from the cache, and it's not dirty, also delete
  #  from underlying store
  if ((!$DidDel || ($DidDel && !($Flags & FC_ISDIRTY)))
     && (my $delete_cb = $Self->{delete_cb})) {
    eval { $delete_cb->($Self->{context}, $_[1]); };
  }
  
  return $DidDel;
}

=item I<clear()>

Clear all items from the cache

Note: If you're using callbacks, this has no effect
on items in the underlying data store. No delete
callbacks are made

=cut
sub clear {
  my $Self = shift;
  $Self->_expunge_all(1, 0);
}

=item I<purge()>

Clear all expired items from the cache

Note: If you're using callbacks, this has no effect
on items in the underlying data store. No delete
callbacks are made, and no write callbacks are made
for the expired data

=cut
sub purge {
  my $Self = shift;
  $Self->_expunge_all(0, 0);
}

=item I<empty($OnlyExpired)>

Empty all items from the cache, or if $OnlyExpired is
true, only expired items.

Note: If 'write_back' mode is enabled, any changed items
are written back to the underlying store. Expired items are
written back to the underlying store as well.

=cut
sub empty {
  my $Self = shift;
  $Self->_expunge_all($_[0] ? 0 : 1, 1);
}

=item I<get_keys($Mode)>

Get a list of keys/values held in the cache. May immediately be out of
date because of the shared access nature of the cache

If $Mode == 0, an array of keys is returned

If $Mode == 1, then an array of hashrefs, with 'key',
'last_access', 'expire_time' and 'flags' keys is returned

If $Mode == 2, then hashrefs also contain 'value' key

=cut
sub get_keys {
  my ($Self, $Cache) = ($_[0], $_[0]->{Cache});

  my $Mode = $_[1] || 0;
  return $Cache->fc_get_keys($Mode)
    if $Mode <= 1 || ($Mode == 2 && $Self->{raw_values});

  # If we're getting values as well, and they're not raw, unfreeze them
  my @Details = $Cache->fc_get_keys(2);
  for (@Details) { $_->{value} = ${thaw($_->{value})}; }
  return @Details;
}

=item I<multi_get($PageKey, [ $Key1, $Key2, ... ])>

The two multi_xxx routines act a bit differently to the
other routines. With the multi_get, you pass a separate
PageKey value and then multiple keys. The PageKey value
is hashed, and that page locked. Then that page is
searched for each key. It returns a hash ref of
Key => Value items found in that page in the cache.

The main advantage of this is just a speed one, if you
happen to need to search for a lot of items on each call.

For instance, say you have users and a bunch of pieces
of separate information for each user. On a particular
run, you need to retrieve a sub-set of that information
for a user. You could do lots of get() calls, or you
could use the 'username' as the page key, and just
use one multi_get() and multi_set() call instead.

A couple of things to note:

=over 4

=item 1.

This makes multi_get()/multi_set() and get()/set()
incompatiable. Don't mix calls to the two, because
you won't find the data you're expecting

=item 2.

The writeback and callback modes of operation do
not work with multi_get()/multi_set(). Don't attempt
to use them together.

=back

=cut
sub multi_get {
  my ($Self, $Cache) = ($_[0], $_[0]->{Cache});

  # Hash value page key, lock page
  my ($HashPage, $HashSlot) = $Cache->fc_hash($_[1]);
  $Cache->fc_lock($HashPage);

  # For each key to find
  my ($Keys, %KVs) = ($_[2]);
  for (@$Keys) {

    # Hash key to get slot in this page and read
    my $FinalKey = "$_[1]-$_";
    (undef, $HashSlot) = $Cache->fc_hash($FinalKey);
    my ($Val, $Flags, $Found) = $Cache->fc_read($HashSlot, $FinalKey);
    next unless $Found;

    # If not using raw values, use thaw() to turn data back into object
    $Val = ${thaw($Val)} unless $Self->{raw_values};

    # Save to return
    $KVs{$_} = $Val;
  }

  # Unlock page and return any found value
  $Cache->fc_unlock();

  return \%KVs;
}

=item I<multi_set($PageKey, { $Key1 => $Value1, $Key2 => $Value2, ... })>

Store specified key/value pair into cache

=cut
sub multi_set {
  my ($Self, $Cache) = ($_[0], $_[0]->{Cache});

  # Hash page key value, lock page
  my ($HashPage, $HashSlot) = $Cache->fc_hash($_[1]);
  $Cache->fc_lock($HashPage);

  # Loop over each key/value storing into this page
  my $KVs = $_[2];
  while (my ($Key, $Val) = each %$KVs) {

    # If not using raw values, use freeze() to turn data 
    $Val = freeze(\$Val) unless $Self->{raw_values};

    # Get key/value len (we've got 'use bytes'), and do expunge check to
    #  create space if needed
    my $FinalKey = "$_[1]-$Key";
    my $KVLen = length($FinalKey) + length($Val);
    $Self->_expunge_page(2, 1, $KVLen);

    # Now hash key and store into page
    (undef, $HashSlot) = $Cache->fc_hash($FinalKey);
    $Cache->fc_write($HashSlot, $FinalKey, $Val, 0);
  }

  # Unlock page
  $Cache->fc_unlock();

  return 1;
}

=back

=cut

=head1 INTERNAL METHODS

=over 4

=cut

=item I<_expunge_all($Mode, $WB)>

Expunge all items from the cache

Expunged items (that have not expired) are written
back to the underlying store if write_back is enabled

=cut
sub _expunge_all {
  my ($Self, $Cache, $Mode, $WB) = ($_[0], $_[0]->{Cache}, $_[1], $_[2]);

  # Repeat expunge for each page
  for (0 .. $Self->{num_pages}-1) {
    $Cache->fc_lock($_);
    $Self->_expunge_page($Mode, $WB, -1);
    $Cache->fc_unlock();
  }

}

=item I<_expunge_page($Mode, $WB, $Len)>

Expunge items from the current page to make space for
$Len bytes key/value items

Expunged items (that have not expired) are written
back to the underlying store if write_back is enabled

=cut
sub _expunge_page {
  my ($Self, $Cache, $Mode, $WB, $Len) = ($_[0], $_[0]->{Cache}, @_[1 .. 3]);

  # If writeback mode, need to get expunged items to write back
  my $write_cb = $Self->{write_back} && $WB ? $Self->{write_cb} : undef;

  my @WBItems = $Cache->fc_expunge($Mode, $write_cb ? 1 : 0, $Len);

  for (@WBItems) {
    next if !($_->{flags} & FC_ISDIRTY);
    eval { $write_cb->($Self->{context}, $_->{key}, $_->{value}, $_->{expire_time}); };
  }
}

sub DESTROY {
  my ($Self, $Cache) = ($_[0], $_[0]->{Cache});

  # Expunge all entries on exit if requested
  if ($Self->{empty_on_exit} && $Cache) {
    $Self->empty();
  }

  if ($Cache) {
    # The destructor calls close for us
    $Cache = undef;
    delete $Self->{Cache};
  }
}

1;

__END__

=back

=head1 SEE ALSO

L<MLDBM::Sync>, L<IPC::MM>, L<Cache::FileCache>, L<Cache::SharedMemoryCache>,
L<DBI>, L<Cache::Mmap>, L<BerkeleyDB>

Latest news/details can also be found at:

L<http://cpan.robm.fastmail.fm/cachefastmmap/>

=head1 AUTHOR

Rob Mueller E<lt>L<mailto:cpan@robm.fastmail.fm>E<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2003-2006 by FastMail IP Partners

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut

