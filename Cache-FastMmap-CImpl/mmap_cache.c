
/*
 * AUTHOR
 *
 * Rob Mueller <cpan@robm.fastmail.fm>
 *
 * COPYRIGHT AND LICENSE
 *
 * Copyright (C) 2003 by FastMail IP Partners
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the same terms as Perl itself. 
 * 
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <stdarg.h>
#include "mmap_cache.h"

#ifdef DEBUG
#define ASSERT(x) assert(x)
#include <assert.h>
#else
#define ASSERT(x)
#endif

/* Cache structure */
struct mmap_cache {

  /* Current page details */
  void * p_base;
  MU32 * p_base_slots;
  MU32    p_cur;
  MU32    p_offset;

  MU32    p_num_slots;
  MU32    p_free_slots;
  MU32    p_old_slots;
  MU32    p_free_data;
  MU32    p_free_bytes;

  int    p_changed;

  /* General page details */
  MU32    c_num_pages;
  MU32    c_page_size;
  MU32    c_size;

  /* Pointer to mmapped area */
  void * mm_var;

  /* Cache general details */
  MU32    start_slots;
  MU32    expire_time;

  /* Share mmap file details */
  int    fh;
  char * share_file;
  int    init_file;
  int    cache_not_found;

  /* Last error string */
  char * last_error;

};

struct mmap_cache_it {
  mmap_cache * cache;
  MU32         p_cur;
  MU32 *       slot_ptr;
  MU32 *       slot_ptr_end;
};


/* Internal functions */
void _mmc_set_error(mmap_cache *, int, char *, ...);
void _mmc_init_pages(mmap_cache *, int);

MU32 * _mmc_find_slot(mmap_cache * , MU32 , void *, int );
void _mmc_delete_slot(mmap_cache * , MU32 *);

int _mmc_check_expunge(mmap_cache * , int);

int  _mmc_test_page(mmap_cache *);
int  _mmc_dump_page(mmap_cache *);

/* Macros to access page entries */
#define PP(p) ((MU32 *)p)

#define P_Magic(p) (*(PP(p)+0))
#define P_NumSlots(p) (*(PP(p)+1))
#define P_FreeSlots(p) (*(PP(p)+2))
#define P_OldSlots(p) (*(PP(p)+3))
#define P_FreeData(p) (*(PP(p)+4))
#define P_FreeBytes(p) (*(PP(p)+5))

#define P_HEADERSIZE 32

/* Macros to access cache slot entries */
#define SP(s) ((MU32 *)s)

/* Offset pointer 'p' by 'o' bytes */
#define PTR_ADD(p,o) ((void *)((char *)p + o))

/* Given a data pointer, get key len, value len or combined len */
#define S_Ptr(b,s)      ((MU32 *)PTR_ADD(b, s))

#define S_LastAccess(s) (*(s+0))
#define S_ExpireTime(s) (*(s+1))
#define S_SlotHash(s)   (*(s+2))
#define S_Flags(s)      (*(s+3))
#define S_KeyLen(s)     (*(s+4))
#define S_ValLen(s)     (*(s+5))

#define S_KeyPtr(s)     ((void *)(s+6))
#define S_ValPtr(s)     (PTR_ADD((void *)(s+6), S_KeyLen(s)))

/* Length of slot data including key and value data */
#define S_SlotLen(s)    (sizeof(MU32)*6 + S_KeyLen(s) + S_ValLen(s))
#define KV_SlotLen(k,v) (sizeof(MU32)*6 + k + v)
/* Found key/val len to nearest 4 bytes */
#define ROUNDLEN(l)     ((l) += 3 - (((l)-1) & 3))

/* Default values for a new cache */
char * def_share_file = "/tmp/sharefile";
MU32    def_init_file = 0;
MU32    def_expire_time = 0;
MU32    def_c_num_pages = 89;
MU32    def_c_page_size = 65536;
MU32    def_start_slots = 89;

/*
 * mmap_cache * mmc_new()
 *
 * Create a new cache object filled with default values. Values may
 * be changed and once ready, you should call mmc_init() to actually
 * open the cache file and mmap it.
 * 
*/
mmap_cache * mmc_new() {
  mmap_cache * cache = (mmap_cache *)malloc(sizeof(mmap_cache));

  cache->mm_var = 0;
  cache->p_cur = -1;

  cache->c_num_pages = def_c_num_pages;
  cache->c_page_size = def_c_page_size;
  cache->c_size = 0;

  cache->start_slots = def_start_slots;
  cache->expire_time = def_expire_time;

  cache->fh = 0;
  cache->share_file = def_share_file;
  cache->init_file = def_init_file;

  return cache;
}

int mmc_set_param(mmap_cache * cache, char * param, char * val) {
  if (!strcmp(param, "init_file")) {
    cache->init_file = atoi(val);
  } else if (!strcmp(param, "page_size")) {
    cache->c_page_size = atoi(val);
  } else if (!strcmp(param, "num_pages")) {
    cache->c_num_pages = atoi(val);
  } else if (!strcmp(param, "expire_time")) {
    cache->expire_time = atoi(val);
  } else if (!strcmp(param, "share_file")) {
    cache->share_file = val;
  } else {
    _mmc_set_error(cache, 0, "Bad set_param parameter: %s", param);
    return -1;
  }

  return 0;
}

int mmc_get_param(mmap_cache * cache, char * param) {
  if (!strcmp(param, "page_size")) {
    return (int)cache->c_page_size;
  } else if (!strcmp(param, "num_pages")) {
    return (int)cache->c_num_pages;
  } else if (!strcmp(param, "expire_time")) {
    return (int)cache->expire_time;
  } else {
    _mmc_set_error(cache, 0, "Bad set_param parameter: %s", param);
    return -1;
  }
}

/*
 * int mmc_init(mmap_cache * cache)
 *
 * Initialise the cache object, opening the share file and mmap'ing any
 * memory.
 * 
*/
int mmc_init(mmap_cache * cache) {
  int res, fh;
  void * mm_var;
  MU32 c_num_pages, c_page_size, c_size, start_slots;
  struct stat statbuf;

  /* Need a share file */
  if (!cache->share_file) {
    _mmc_set_error(cache, 0, "No share file specified");
    return -1;
  }

  /* Basic cache params */
  c_num_pages = cache->c_num_pages;
  ASSERT(c_num_pages >= 1 && c_num_pages <= 1000);

  c_page_size = cache->c_page_size;
  ASSERT(c_page_size >= 1024 && c_page_size <= 1024*1024);

  start_slots = cache->start_slots;
  ASSERT(start_slots >= 10 && start_slots <= 500);

  cache->c_size = c_size = c_num_pages * c_page_size;

  /* Check if file exists */
  res = stat(cache->share_file, &statbuf);

  /* Remove if different size or remove requested */
  if (!res &&
      (cache->init_file || (statbuf.st_size != c_size))) {
    res = remove(cache->share_file);
    if (res == -1 && errno != ENOENT) {
      _mmc_set_error(cache, errno, "Unlink of existing share file %s failed", cache->share_file);
      return -1;
    }
  }

  /* Create file if it doesn't exist */
  res = stat(cache->share_file, &statbuf);
  if (res == -1) {
    res = open(cache->share_file, O_WRONLY | O_CREAT | O_EXCL | O_TRUNC | O_APPEND, 0640);
    if (res == -1) {
      _mmc_set_error(cache, errno, "Create of share file %s failed", cache->share_file);
      return -1;
    }

    _mmc_init_pages(cache, res);

    close(res);
  }

  /* Open for reading/writing */
  fh = open(cache->share_file, O_RDWR);
  if (fh == -1) {
    _mmc_set_error(cache, errno, "Open of share file %s failed", cache->share_file);
    return -1;
  }

  /* Map file into memory */
  mm_var = mmap(0, c_size, PROT_READ | PROT_WRITE, MAP_SHARED, fh, 0);
  if (mm_var == (void *)MAP_FAILED) {
    close(fh);
    _mmc_set_error(cache, errno, "Mmap of shared file %s failed", cache->share_file);
    return -1;
  }

  cache->fh = fh;
  cache->mm_var = mm_var;

  return 0;
}

/*
 * int mmc_close(mmap_cache * cache)
 *
 * Close the given cache, unmmap'ing any memory and closing file
 * descriptors. 
 * 
*/
int mmc_close(mmap_cache *cache) {
  int res;

  /* Shouldn't call if not init'ed */
  ASSERT(cache->fh);
  ASSERT(cache->mm_var);

  /* Shouldn't call if page still locked */
  ASSERT(cache->p_cur == -1);

  /* Unlock any locked page */
  if (cache->p_cur != -1) {
    mmc_unlock(cache);
  }

  /* Close file */
  if (cache->fh) {
    close(cache->fh);
  }

  /* Unmap memory */
  if (cache->mm_var) {
    res = munmap(cache->mm_var, cache->c_size);
    if (res == -1) {
      _mmc_set_error(cache, errno, "Mmap of shared file %s failed", cache->share_file);
      return -1;
    }
  }

  free(cache);

  return 0;
}

char * mmc_error(mmap_cache * cache) {
  return cache->last_error;
}

/*
 * mmc_lock(
 *   cache_mmap * cache, MU32 p_cur
 * )
 *
 * Lock the given page number using fcntl locking. Setup
 * cache->p_* fields with correct values for the given page
 *
*/
int mmc_lock(mmap_cache * cache, MU32 p_cur) {
  struct flock lock;
  int old_alarm, alarm_left = 10;
  int lock_res = -1;
  void * p_ptr;

  /* Setup page details */
  cache->p_cur = p_cur;
  cache->p_offset = p_cur * cache->c_page_size;
  p_ptr = PTR_ADD(cache->mm_var, cache->p_offset);
 
  /* Setup fcntl locking structure */
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = cache->p_offset;
  lock.l_len = cache->c_page_size;

  old_alarm = alarm(alarm_left);

  while (lock_res != 0) {

    /* Lock the page (block till done, signal, or timeout) */
    lock_res = fcntl(cache->fh, F_SETLKW, &lock);

    /* Continue immediately if success */
    if (lock_res == 0) {
      alarm(old_alarm);
      break;
    }

    /* Turn off alarm for a moment */
    alarm_left = alarm(0);

    /* Some signal interrupted, and it wasn't the alarm? Rerun lock */
    if (lock_res == -1 && errno == EINTR && alarm_left) {
      alarm(alarm_left);
      continue;
    }

    /* Lock failed? */
    _mmc_set_error(cache, errno, "Lock failed");
    alarm(old_alarm);
    return -1;
  }

#ifdef DEBUG
  /* Check page header */
  {
  ASSERT(P_Magic(p_ptr) == 0x92f7e3b1);
  ASSERT(P_NumSlots(p_ptr) >= 89 && P_NumSlots(p_ptr) < cache->c_page_size);
  ASSERT(P_FreeSlots(p_ptr) > 0 && P_FreeSlots(p_ptr) <= P_NumSlots(p_ptr));
  ASSERT(P_OldSlots(p_ptr) <= P_FreeSlots(p_ptr));
  ASSERT(P_FreeData(p_ptr) + P_FreeBytes(p_ptr) == cache->c_page_size);
  }
#endif

  /* Copy to cache structure */
  cache->p_num_slots = P_NumSlots(p_ptr);
  cache->p_free_slots = P_FreeSlots(p_ptr);
  cache->p_old_slots = P_OldSlots(p_ptr);
  cache->p_free_data = P_FreeData(p_ptr);
  cache->p_free_bytes = P_FreeBytes(p_ptr);

  /* Setup page pointers */
  cache->p_base = p_ptr;
  cache->p_base_slots = PTR_ADD(p_ptr, P_HEADERSIZE);

  ASSERT(_mmc_test_page(cache));

  return 0;
}

/*
 * mmc_unlock(
 *   cache_mmap * cache
 * )
 *
 * Unlock any currently locked page
 *
*/
int mmc_unlock(mmap_cache * cache) {
  struct flock lock;

  ASSERT(cache->p_cur != -1);

  /* If changed, save page header changes back */
  if (cache->p_changed) {
    void * p_ptr = cache->p_base;

    /* Save any changed information back to page */
    P_NumSlots(p_ptr) = cache->p_num_slots;
    P_FreeSlots(p_ptr) = cache->p_free_slots;
    P_OldSlots(p_ptr) = cache->p_old_slots;
    P_FreeData(p_ptr) = cache->p_free_data;
    P_FreeBytes(p_ptr) = cache->p_free_bytes;
  }

  /* Test before unlocking */
  ASSERT(_mmc_test_page(cache));

  /* Setup fcntl locking structure */
  lock.l_type = F_UNLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = cache->p_offset;
  lock.l_len = cache->c_page_size;

  /* And unlock page */
  fcntl(cache->fh, F_SETLKW, &lock);

  /* Set to bad value while page not locked */
  cache->p_cur = -1;

  return 0;
}


/*
 * int mmc_hash(
 *   cache_mmap * cache,
 *   void *key_ptr, int key_len,
 *   MU32 *hash_page, MU32 *hash_slot
 * )
 *
 * Hashes the given key, and returns hash value, hash page and hash
 * slot part
 *
*/
int mmc_hash(
  mmap_cache *cache,
  void *key_ptr, int key_len,
  MU32 *hash_page, MU32 *hash_slot
) {
  MU32 h = 0x92f7e3b1;
  unsigned char * uc_key_ptr = (unsigned char *)key_ptr;
  unsigned char * uc_key_ptr_end = uc_key_ptr + key_len;

  while (uc_key_ptr != uc_key_ptr_end) {
    h = (h << 4) + (h >> 28) + *uc_key_ptr++;
  }

  *hash_page = h % cache->c_num_pages;
  *hash_slot = h / cache->c_num_pages;

  return 0;
}

/*
 * int mmc_read(
 *   cache_mmap * cache, MU32 hash_slot,
 *   void *key_ptr, int key_len,
 *   void **val_ptr, int *val_len,
 *   MU32 *flags
 * )
 *
 * Read key from current page
 *
*/
int mmc_read(
  mmap_cache *cache, MU32 hash_slot,
  void *key_ptr, int key_len,
  void **val_ptr, int *val_len,
  MU32 *flags
) {
  /* Search slots for key */
  MU32 * slot_ptr = _mmc_find_slot(cache, hash_slot, key_ptr, key_len);

  /* Did we find a value? */
  if (*slot_ptr == 0) {

    /* Return -1 if not */
    return -1;

  /* We found it! Check some other things... */
  } else {

    MU32 * base_det = S_Ptr(cache->p_base, *slot_ptr);
    MU32 now = (MU32)time(0);

    MU32 expire_time = S_ExpireTime(base_det);

    /* Sanity check hash matches */
    ASSERT(S_SlotHash(base_det) == hash_slot);

    /* Value expired? */
    if (expire_time && now > expire_time) {

      /* Delete slot and return not found */
      _mmc_delete_slot(cache, slot_ptr);
      ASSERT(*slot_ptr == 1);

      return -1;
    }

    /* Update hit time */
    S_LastAccess(base_det) = now;

    /* Copy values to pointers */
    *flags = S_Flags(base_det);
    *val_len = S_ValLen(base_det);
    *val_ptr = S_ValPtr(base_det);

    return 0;
  }
}

/*
 * int mmc_write(
 *   cache_mmap * cache, MU32 hash_slot,
 *   void *key_ptr, int key_len,
 *   void *val_ptr, int val_len,
 *   MU32 flags
 * )
 *
 * Write key to current page
 *
*/
int mmc_write(
  mmap_cache *cache, MU32 hash_slot,
  void *key_ptr, int key_len,
  void *val_ptr, int val_len,
  MU32 flags
) {
  int did_store = 0;
  MU32 kvlen = KV_SlotLen(key_len, val_len);

  /* Search for slot with given key */
  MU32 * slot_ptr = _mmc_find_slot(cache, hash_slot, key_ptr, key_len);

  ROUNDLEN(kvlen);

  ASSERT(cache->p_cur != -1);

  /* If found, delete slot for new value */
  if (*slot_ptr != 0) {
    _mmc_delete_slot(cache, slot_ptr);
    ASSERT(*slot_ptr == 1);
  }

  ASSERT(*slot_ptr <= 1);

  /* If there's space, store the key/value in the data section */
  if (cache->p_free_bytes >= kvlen) {
    MU32 * base_det = PTR_ADD(cache->p_base, cache->p_free_data);
    MU32 now = (MU32)time(0);

    /* Calculate expiry time */
    MU32 expire_time = cache->expire_time ? now + cache->expire_time : 0;

    /* Store info into slot */
    S_LastAccess(base_det) = now;
    S_ExpireTime(base_det) = expire_time;
    S_SlotHash(base_det) = hash_slot;
    S_Flags(base_det) = flags;
    S_KeyLen(base_det) = (MU32)key_len;
    S_ValLen(base_det) = (MU32)val_len;

    /* Copy key/value to data section */
    memcpy(S_KeyPtr(base_det), key_ptr, key_len);
    memcpy(S_ValPtr(base_det), val_ptr, val_len);

    /* Update used slots/free data info */
    cache->p_free_slots--;
    if (*slot_ptr == 1) { cache->p_old_slots--; }

    /* Save new data offset */
    *slot_ptr = cache->p_free_data;

    /* Update free space */
    cache->p_free_bytes -= kvlen;
    cache->p_free_data += kvlen;

    /* Ensure changes are saved back */
    cache->p_changed = 1;

    did_store = 1;
  }

  return did_store;
}

/*
 * int mmc_delete(
 *   cache_mmap * cache, MU32 hash_slot,
 *   void *key_ptr, int key_len
 * )
 *
 * Delete key from current page
 *
*/
int mmc_delete(
  mmap_cache *cache, MU32 hash_slot,
  void *key_ptr, int key_len,
  MU32 * flags
) {
  /* Search slots for key */
  MU32 * slot_ptr = _mmc_find_slot(cache, hash_slot, key_ptr, key_len);

  /* Did we find a value? */
  if (*slot_ptr == 0) {

    /* Return 0 if not deleted */
    return 0;

  /* We found it, delete it */
  } else {

    /* Store flags in output pointer */
    MU32 * base_det = S_Ptr(cache->p_base, *slot_ptr);
    *flags = S_Flags(base_det);

    _mmc_delete_slot(cache, slot_ptr);
    return 1;
  }

}

int last_access_cmp(const void * a, const void * b) {
  return S_LastAccess(*(MU32 **)a) < S_LastAccess(*(MU32 **)b);
}

/*
 * int mmc_calc_expunge(
 *   cache_mmap * cache, int mode, int len, MU32 * new_num_slots, MU32 *** to_expunge
 * )
 *
 * Calculate entries to expunge from current page.
 *
 * If mode == 0, only expired items are expunged
 * If mode == 1, all entries are expunged
 * If mode == 2, 
 *   If len < 0, entries are expunged till 40% free space is created
 *   If len >= 0, and space available for len bytes, nothing is expunged
 *   If len >= 0, and not enough space, entries are expunged till 40% free
 *
 * If expunged is non-null pointer, result is filled with
 * a list of slots to expunge
 *
 * Return value is number of items to expunge
 *
*/
int mmc_calc_expunge(
  mmap_cache * cache,
  int mode, int len,
  MU32 * new_num_slots, MU32 *** to_expunge
) {
  /* Length of key/value data when stored */
  MU32 kvlen = KV_SlotLen(len, 0);
  ROUNDLEN(kvlen);

  ASSERT(cache->p_cur != -1);

  /* If len >= 0, and space available for len bytes, nothing is expunged */
  if (mode == 2 && len >= 0) {

    if (
      /* Hash table more than 30% free slots? */
      ((double)(cache->p_free_slots - cache->p_old_slots) / cache->p_num_slots > 0.3) &&
      /* And enough free space? */
      (cache->p_free_bytes >= kvlen) 
       )
    {
      /* Nothing to do */
      return 0;
    }
  }

  {
    MU32 num_slots = cache->p_num_slots;

    MU32 used_slots = num_slots - cache->p_free_slots;
    MU32 * slot_ptr = cache->p_base_slots;
    MU32 * slot_end = slot_ptr + num_slots;

    /* Store pointers to used slots */
    MU32 ** copy_base_det = (MU32 **)malloc(sizeof(MU32 *) * used_slots);
    MU32 ** copy_base_det_end = copy_base_det + used_slots;
    MU32 ** copy_base_det_out = copy_base_det;
    MU32 ** copy_base_det_in = copy_base_det + used_slots;

    MU32 page_data_size = cache->c_page_size - num_slots * 4 - P_HEADERSIZE;
    MU32 in_slots, data_thresh, used_data = 0;
    MU32 now = (MU32)time(0);

    /* Loop for each existing slot, and store in a list */
    for (; slot_ptr != slot_end; slot_ptr++) {
      MU32 data_offset = *slot_ptr;
      MU32 * base_det = S_Ptr(cache->p_base, data_offset);
      MU32 expire_time, kvlen;

      /* Ignore if if free slot */
      if (data_offset <= 1) {
        continue;
      }

      /* Definitely out if mode == 1 which means expunge all */
      if (mode == 1) {
        *copy_base_det_out++ = base_det;
        continue;
      }

      /* Definitely out if expired */
      expire_time = S_ExpireTime(base_det);
      if (expire_time && now >= expire_time) {
        *copy_base_det_out++ = base_det;
        continue;
      }

      /* If mode == 2, track used space */
      kvlen = S_SlotLen(base_det);
      ROUNDLEN(kvlen);
      ASSERT(kvlen <= page_data_size);

      /* Keep track of total data used */
      used_data += kvlen;
      ASSERT(used_data <= page_data_size);

      /* Potentially in */
      *--copy_base_det_in = base_det;
    }

    /* Check that definitely in and out slots add up to used slots */
    ASSERT(copy_base_det_in == copy_base_det_out);
    ASSERT(mode != 1 || copy_base_det_out == copy_base_det_end);

    /* Calculate number of new slots */
    if ((double)(copy_base_det_end - copy_base_det_out) / num_slots > 0.3) {
      num_slots = (num_slots * 2) + 1;
    }
    page_data_size = cache->c_page_size - num_slots * 4 - P_HEADERSIZE;

    /* If mode == 0 or 1, we've just worked out ones to keep and
     *  which to dispose of, so return results */
    if (mode == 0 || mode == 1) {
      *to_expunge = copy_base_det;
      *new_num_slots = num_slots;
      return (copy_base_det_out - copy_base_det);
    }

    /* mode == 2, sort by last access, and remove till enough free space */

    /* Sort those potentially in by last access */
    in_slots = copy_base_det_end - copy_base_det_in;
    qsort((void *)copy_base_det_in, in_slots, sizeof(MU32 *), &last_access_cmp);

    /* Throw out old slots till we have 40% free data space */
    data_thresh = (MU32)(0.6 * page_data_size);

    while (copy_base_det_in != copy_base_det_end && used_data >= data_thresh) {
      MU32 * slot_ptr = *copy_base_det_in;
      MU32 kvlen = S_SlotLen(slot_ptr);
      ROUNDLEN(kvlen);
      ASSERT(kvlen <= page_data_size);
      used_data -= kvlen;

      ASSERT(used_data >= 0);

      copy_base_det_out = ++copy_base_det_in;
    }
    ASSERT(used_data < page_data_size);

    *to_expunge = copy_base_det;
    *new_num_slots = num_slots;
    return (copy_base_det_out - copy_base_det);
  }
}

/*
 * int mmc_do_expunge(
 *   cache_mmap * cache, int num_expunge, MU32 new_num_slots, MU32 ** to_expunge
 * )
 *
 * Expunge given entries from current page.
 *
*/
int mmc_do_expunge(
  mmap_cache * cache,
  int num_expunge, MU32 new_num_slots, MU32 ** to_expunge
) {
  MU32 * base_slots = cache->p_base_slots;

  MU32 ** to_keep = to_expunge + num_expunge;
  MU32 ** to_keep_end = to_expunge + (cache->p_num_slots - cache->p_free_slots);
  MU32 new_used_slots = (to_keep_end - to_keep);

  /* Build new slots data and KV data */
  MU32 slot_data_size = new_num_slots * 4;
  MU32 * new_slot_data = (MU32 *)malloc(slot_data_size);

  MU32 page_data_size = cache->c_page_size - new_num_slots * 4 - P_HEADERSIZE;

  void * new_kv_data = malloc(page_data_size);
  MU32 new_offset = 0;

  /* Start all new slots empty */
  memset(new_slot_data, 0, slot_data_size);

  /* Copy entries to keep to new slot entires and data sections */
  for (;to_keep < to_keep_end; to_keep++) {
    MU32 * old_base_det = *to_keep;
    MU32 * new_slot_ptr;
    MU32 kvlen;

    /* Hash key to find starting slot */
    MU32 slot = S_SlotHash(old_base_det) % new_num_slots;

#ifdef DEBUG
    /* Check hash actually matches stored value */
    {
      MU32 hash_page_dummy, hash_slot;
      mmc_hash(cache, S_KeyPtr(old_base_det), S_KeyLen(old_base_det), &hash_page_dummy, &hash_slot);

      ASSERT(hash_slot == S_SlotHash(old_base_det));
    }
#endif

    /* Find free slot */
    new_slot_ptr = new_slot_data + slot;
    while (*new_slot_ptr) {
      if (++slot >= new_num_slots) { slot = 0; }
      new_slot_ptr = new_slot_data + slot;
    }

    /* Copy slot and KV data */
    kvlen = S_SlotLen(old_base_det);
    memcpy(PTR_ADD(new_kv_data, new_offset), old_base_det, kvlen);

    /* Store slot data and mark as used */
    *new_slot_ptr = new_offset + new_num_slots * 4 + P_HEADERSIZE;

    ROUNDLEN(kvlen);
    new_offset += kvlen;
  }

  ASSERT(new_offset <= page_data_size);

/*  printf("page=%d\n", cache->p_cur);
  printf("old_slots=%d, new_slots=%d\n", old_num_slots, new_num_slots);
  printf("old_used_slots=%d, new_used_slots=%d\n", old_used_slots, new_used_slots);*/

  /* Store back into mmap'ed file space */
  memcpy(base_slots, new_slot_data, slot_data_size);
  memcpy(base_slots + new_num_slots, new_kv_data, new_offset);

  cache->p_num_slots = new_num_slots;
  cache->p_free_slots = new_num_slots - new_used_slots;
  cache->p_old_slots = 0;
  cache->p_free_data = new_offset + new_num_slots * 4 + P_HEADERSIZE;
  cache->p_free_bytes = page_data_size - new_offset;

  /* Make sure changes are saved back to mmap'ed file */
  cache->p_changed = 1;

  /* Free allocated memory */
  free(new_kv_data);
  free(new_slot_data);
  free(to_expunge);

  ASSERT(_mmc_test_page(cache));

  return 0;
}

/*
 * mmap_cache_it * mmc_iterate_new(mmap_cache * cache)
 *
 * Setup a new iterator to iterate over stored items
 * in the cache
 *
*/
mmap_cache_it * mmc_iterate_new(mmap_cache * cache) {
  mmap_cache_it * it = (mmap_cache_it *)malloc(sizeof(mmap_cache_it));
  it->cache = cache;
  it->p_cur = -1;
  it->slot_ptr = 0;
  it->slot_ptr_end = 0;

  return it;
}

/*
 * MU32 * mmc_iterate_next(mmap_cache_it * it)
 *
 * Move iterator to next item in the cache and return
 * pointer to details (0 if there is no next).
 *
 * You can retrieve details with mmc_get_details(...)
 *
*/
MU32 * mmc_iterate_next(mmap_cache_it * it) {
  mmap_cache * cache = it->cache;
  MU32 * slot_ptr = it->slot_ptr;
  MU32 * base_det;

  /* If empty slot, keep moving till we find a used one */
  while (slot_ptr == it->slot_ptr_end || *slot_ptr <= 1) {

    /* End of page ... */
    if (slot_ptr == it->slot_ptr_end) {

      /* Unlock current page if any */
      if (it->p_cur != -1) {
        mmc_unlock(it->cache);
      }

      /* Move to the next page, return 0 if no more pages */
      if (++it->p_cur == cache->c_num_pages) {
        it->p_cur = -1;
        it->slot_ptr = 0;
        return 0;
      }

      /* Lock the new page number */
      mmc_lock(it->cache, it->p_cur);

      /* Setup new pointers */
      slot_ptr = cache->p_base_slots;
      it->slot_ptr_end = slot_ptr + cache->p_num_slots;

      /* Check again */
      continue;
    }

    /* Move to next slot */
    slot_ptr++;
  }

  /* Get pointer to details for this entry */
  base_det = S_Ptr(cache->p_base, *slot_ptr);

  /* Move to the next slot for next iteration */
  it->slot_ptr = ++slot_ptr;

  /* Return that we found the next item */
  return base_det;
}

/*
 * void mmc_iterate_close(mmap_cache_it * it)
 *
 * Finish and dispose of iterator memory
 *
*/
void mmc_iterate_close(mmap_cache_it * it) {
  /* Unlock page if locked */
  if (it->p_cur != -1) {
    mmc_unlock(it->cache);
  }

  /* Free memory */
  free(it);
}

/*
 * void mmc_get_details(
 *   mmap_cache * cache,
 *   MU32 * base_det,
 *   void ** key_ptr, int * key_len,
 *   void ** val_ptr, int * val_len,
 *   MU32 * last_access, MU32 * expire_time, MU32 * flags
 * )
 *
 * Given a base_det pointer to entries details
 * (as returned by mmc_iterate_next(...) and
 * mmc_calc_expunge(...)) return details of that
 * entry in the cache
 *
*/
void mmc_get_details(
  mmap_cache * cache,
  MU32 * base_det,
  void ** key_ptr, int * key_len,
  void ** val_ptr, int * val_len,
  MU32 * last_access, MU32 * expire_time, MU32 * flags
) {
  cache = cache;

  *key_ptr = S_KeyPtr(base_det);
  *key_len = S_KeyLen(base_det);

  *val_ptr = S_ValPtr(base_det);
  *val_len = S_ValLen(base_det);

  *last_access = S_LastAccess(base_det);
  *expire_time = S_ExpireTime(base_det);
  *flags = S_Flags(base_det);
}


/*
 * _mmc_delete_slot(
 *   mmap_cache * cache, MU32 * slot_ptr
 * )
 *
 * Delete details from the given slot
 *
*/
void _mmc_delete_slot(
  mmap_cache * cache, MU32 * slot_ptr
) {
  ASSERT(*slot_ptr > 1);
  ASSERT(cache->p_cur != -1);

  /* Set offset to 1 */
  *slot_ptr = 1;

  /* Increase slot free counters */
  cache->p_free_slots++;
  cache->p_old_slots++;

  /* Ensure changes are saved back */
  cache->p_changed = 1;
}

/*
 * MU32 * _mmc_find_slot(
 *   mmap_cache * cache, MU32 hash_slot,
 *   void *key_ptr, int key_len
 * )
 *
 * Search current page for a particular 'key'. Use 'hash_slot' to
 * calculate starting slot. Return pointer to slot.
 *
*/
MU32 * _mmc_find_slot(
  mmap_cache * cache, MU32 hash_slot,
  void *key_ptr, int key_len
) {
  MU32 slots_left, * slots_end;
  /* Modulo hash_slot to find starting slot */
  MU32 * slot_ptr = cache->p_base_slots + (hash_slot % cache->p_num_slots);

  /* Total slots and pointer to end of slot data to do wrapping */
  slots_left = cache->p_num_slots;
  slots_end = cache->p_base_slots + slots_left;

  ASSERT(cache->p_cur != -1);

  /* Loop with integer probing till we find or don't */
  while (slots_left--) {
    MU32 data_offset = *slot_ptr;
    ASSERT(data_offset == 0 || data_offset == 1 ||
        ((data_offset >= P_HEADERSIZE + cache->p_num_slots*4) &&
         (data_offset < cache->c_page_size) &&
         ((data_offset & 3) == 0)));

    /* data_offset == 0 means empty slot, and no more beyond */
    if (data_offset == 0) {

      /* Return pointer to last checked slot */
      return slot_ptr;
    }

    if (data_offset == 1) {
      /* data_offset == 1 means empty entry, but possibly more past this one */

    } else {
      /* Offset is from start of data area */
      MU32 * base_det = S_Ptr(cache->p_base, data_offset);

      /* Two longs are key len and data len */
      MU32 fkey_len = S_KeyLen(base_det);

      /* Key matches? */
      if (fkey_len == (MU32)key_len && !memcmp(key_ptr, S_KeyPtr(base_det), key_len)) {

        /* Yep, found it! */
        return slot_ptr;
      }
    }

    /* Linear probe and wrap at end of slot data... */
    if (++slot_ptr == slots_end) { slot_ptr = cache->p_base_slots; }
    ASSERT(slot_ptr >= cache->p_base_slots && slot_ptr < slots_end);
  }

  ASSERT(0);
  return 0;
}

/*
 * void _mmc_init_pages(mmap_cache * cache, int fh)
 *
 * Initialise the file descriptor passed with empty pages
 *
*/
void _mmc_init_pages(mmap_cache * cache, int fh) {
  int p;
  void * page_data = (void *)malloc(cache->c_page_size);
  memset(page_data, 0, cache->c_page_size);

  P_Magic(page_data) = 0x92f7e3b1;
  P_NumSlots(page_data) = cache->start_slots;
  P_FreeSlots(page_data) = cache->start_slots;
  P_OldSlots(page_data) = 0;
  P_FreeData(page_data) = P_HEADERSIZE + cache->start_slots * 4;
  P_FreeBytes(page_data) = cache->c_page_size - P_FreeData(page_data);

  /* Write pages */
  for (p = 0; p < cache->c_num_pages; p++) {
    write(fh, page_data, cache->c_page_size);
  }

  free(page_data);
}

/*
 * void _mmc_set_error(mmap_cache *cache, int err, char * error_string, ...)
 *
 * Set internal error string/state
 *
*/
void _mmc_set_error(mmap_cache *cache, int err, char * error_string, ...) {
  va_list ap;
  static char errbuf[1024];

  va_start(ap, error_string);

  /* Make sure it's terminated */
  errbuf[1023] = '\0';

  /* Start with error string passed */
  vsnprintf(errbuf, 1023, error_string, ap);

  /* Add system error code if passed */
  if (err) {
    strncat(errbuf, ": ", 1024);
    strncat(errbuf, strerror(err), 1023);
  }

  /* Save in cache object */
  cache->last_error = errbuf;

  va_end(ap);

  return;
}

/*
 * int _mmc_test_page(mmap_cache * cache)
 *
 * Test integrity of current page
 *
*/
int  _mmc_test_page(mmap_cache * cache) {
  MU32 * slot_ptr = cache->p_base_slots;
  MU32 count_free = 0, count_old = 0, max_data_offset = 0;
  MU32 data_size = cache->c_page_size;

  ASSERT(cache->p_cur != -1);

  for (; slot_ptr < cache->p_base_slots + cache->p_num_slots; slot_ptr++) {
    MU32 data_offset = *slot_ptr;

    ASSERT(data_offset == 0 || data_offset == 1 ||
        (data_offset >= P_HEADERSIZE + cache->p_num_slots * 4 &&
         data_offset < cache->c_page_size));

    if (data_offset == 1) {
      count_old++;
    }
    if (data_offset <= 1) {
      count_free++;
      continue;
    }

    if (data_offset > 1) {
      MU32 * base_det = S_Ptr(cache->p_base, data_offset);

      MU32 last_access = S_LastAccess(base_det);
      MU32 expire_time = S_ExpireTime(base_det);
      MU32 key_len = S_KeyLen(base_det);
      MU32 val_len = S_ValLen(base_det);
      MU32 kvlen = S_SlotLen(base_det);
      ROUNDLEN(kvlen);

      ASSERT(last_access > 1000000000 && last_access < 1200000000);
      ASSERT(expire_time == 0 || (expire_time > 1000000000 && expire_time < 1200000000));

      ASSERT(key_len >= 0 && key_len < data_size);
      ASSERT(val_len >= 0 && val_len < data_size);
      ASSERT(kvlen >= 4*4 && kvlen < data_size);

      /* Keep track of largest end of data position */
      if (data_offset + kvlen > max_data_offset) {
        max_data_offset = data_offset + kvlen;
      }

      /* Check if key lookup finds same thing */
      {
        MU32 hash_page, hash_slot, * find_slot_ptr;

        /* Hash it */
        mmc_hash(cache, S_KeyPtr(base_det), (int)key_len,
          &hash_page, &hash_slot);
        ASSERT(hash_slot == S_SlotHash(base_det));

        find_slot_ptr = _mmc_find_slot(cache, hash_slot, S_KeyPtr(base_det), key_len);

        ASSERT(find_slot_ptr == slot_ptr);
      }

    }

  }

  ASSERT(count_free == cache->p_free_slots);
  ASSERT(count_old == cache->p_old_slots);
  ASSERT(cache->p_free_data >= max_data_offset);

  return 1;
}

/*
 * int _mmc_dump_page(mmap_cache * cache)
 *
 * Dump text version of current page to STDOUT
 *
*/
int _mmc_dump_page(mmap_cache * cache) {
  MU32 slot;

  ASSERT(cache->p_cur != -1);

  printf("PageNum: %d\n", cache->p_cur);
  printf("\n");
  printf("PageSize: %d\n", cache->c_page_size);
  printf("BasePage: %p\n", cache->p_base);
  printf("BaseSlots: %p\n", cache->p_base_slots);
  printf("\n");
  printf("NumSlots: %d\n", cache->p_num_slots);
  printf("FreeSlots: %d\n", cache->p_free_slots);
  printf("OldSlots: %d\n", cache->p_old_slots);
  printf("FreeData: %d\n", cache->p_free_data);
  printf("FreeBytes: %d\n", cache->p_free_bytes);

  for (slot = 0; slot < cache->p_num_slots; slot++) {
    MU32 * slot_ptr = cache->p_base_slots + slot;

    printf("Slot: %d; OF=%d; ", slot, *slot_ptr);

    if (*slot_ptr > 1) {
      MU32 * base_det = S_Ptr(cache->p_base, *slot_ptr);
      MU32 key_len = S_KeyLen(base_det);
      MU32 val_len = S_ValLen(base_det);
      char key[256], val[256];

      printf("LA=%d, ET=%d, HS=%d, FL=%d\n",
        S_LastAccess(base_det), S_ExpireTime(base_det),
        S_SlotHash(base_det), S_Flags(base_det));

      /* Get data */
      memcpy(S_KeyPtr(base_det), key, key_len > 256 ? 256 : key_len);
      key[key_len] = 0;
      memcpy(S_ValPtr(base_det), val, val_len > 256 ? 256 : val_len);
      val[val_len] = 0;

      printf("  K=%s, V=%s\n", key, val);

    }

  }

  return 0;
}


