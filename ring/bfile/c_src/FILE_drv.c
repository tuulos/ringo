/* Interface to stdio buffered FILE io                              */
/* author: klacke@kaja.klacke.net                                   */
/* Created : 22 Nov 1999 by Claes Wikstrom <klacke@kaja.klacke.net> */

#include <string.h>

#define USE_STDIO

#ifdef WIN32
#define USE_STDIO
#include <windows.h>

#else

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <ctype.h>
#endif

#include "erl_driver.h"
#ifndef ERL_DRV_NIL
#include "erl_driver_compat.h"
#endif

#ifdef USE_STDIO
#  include <stdio.h>
#else

#  define malloc(s)     driver_alloc(s)
#  define realloc(p, s) driver_realloc(p, s)
#  define free(p)       driver_free(p)

#  define BINTERFACE static
#  include "bbio.c"
#  define FILE    bFILE
#  define clearerr bclearerr
#  define fclose  bfclose
#  define feof    bfeof
#  define ferror  bferror
#  define fflush  bfflush
#  define fgets   bfgets
#  define fileno  bfileno
#  define fopen   bfopen
#  define fread   bfread
#  define fseek   bfseek
#  define ftell   bftell
#  define fwrite  bfwrite
#  define getc    bgetc
#  define ungetc  bungetc
#endif


#define get_int32(s) ((((unsigned char*) (s))[0] << 24) | \
                      (((unsigned char*) (s))[1] << 16) | \
                      (((unsigned char*) (s))[2] << 8)  | \
                      (((unsigned char*) (s))[3]))

#define put_int32(i, s) {((char*)(s))[0] = (char)((i) >> 24) & 0xff; \
                        ((char*)(s))[1] = (char)((i) >> 16) & 0xff; \
                        ((char*)(s))[2] = (char)((i) >> 8)  & 0xff; \
                        ((char*)(s))[3] = (char)((i)        & 0xff);}
/* op codes */

#define XX_OPEN             'o'
#define XX_CLOSE            'c'
#define XX_READ             'r'
#define XX_WRITE            'w'
#define XX_SEEK             's'
#define XX_TELL             't'
#define XX_TRUNCATE         'T'
#define XX_FLUSH            'f'
#define XX_OEOF             'e'
#define XX_ERROR            'E'
#define XX_GETC             'g'
#define XX_GETS             'G'
#define XX_GETS2            '2'
#define XX_SET_LINEBUF_SIZE 'S'
#define XX_UNGETC           'u'



/* return codes */
#define XX_VALUE   'v'
#define XX_FLINE   'L'
#define XX_OK      'o'
#define XX_I32     'O'
#define XX_NOLINE  'N'
#define XX_FERROR  'E'
#define XX_REOF    'x'


#ifdef WIN32
#define XX_EINVAL WSAEINVAL


#else
#define XX_EINVAL EINVAL

#endif


static ErlDrvData FILE_start(ErlDrvPort port, char *buf);
static void FILE_stop(ErlDrvData drv_data);
static void FILE_from_erlangv(ErlDrvData drv_data,  ErlIOVec* ev);

static ErlDrvEntry FILE_driver_entry;

typedef struct _desc {
    ErlDrvPort port;
    FILE *fp;
    int linebuf_size;
} Desc;


static ErlDrvData FILE_start(ErlDrvPort port, char *buf)
{
    Desc *d = (Desc*) driver_alloc(sizeof (Desc));

    if (d == NULL) 
	return (ErlDrvData) -1;
    d->fp = NULL;
    d->port = port;
    d->linebuf_size = 255;   /* default line size */
    return (ErlDrvData) d;
}


static void FILE_stop(ErlDrvData drv_data)
{
    Desc *d = (Desc*) drv_data;
    if (d->fp)
	fclose(d->fp);
    driver_free(d);
}

/*
** FIXME: use driver_vec_to_buf insted,
** this is a patch until next release of otp
*/
static int vec_to_buf(ErlIOVec* vec, char* buf, int len)
{
    SysIOVec* iov = vec->iov;
    int n = vec->vsize;
    int orig_len = len;

    while(n--) {
	int ilen = iov->iov_len;
	if (ilen < len) {
	    memcpy(buf, iov->iov_base, ilen);
	    len -= ilen;
	    buf += ilen;
	    iov++;   /* this line was miising */
	}
	else {
	    memcpy(buf, iov->iov_base, len);
	    return orig_len;
	}
    }
    return (orig_len - len);
}


static int driver_error(ErlDrvPort port, int err)
{
    char response[256];		/* Response buffer. */
    char* s;
    char* t;

    response[0] = XX_FERROR;
    for (s = erl_errno_id(err), t = response+1; *s; s++, t++)
	*t = tolower(*s);
    driver_output2(port, response, t-response, NULL, 0);
    return 0;
}

static void driver_ret32(ErlDrvPort port, unsigned int r)
{
    char ch = XX_I32;
    unsigned char rbuf[4];

    put_int32(r, rbuf);
    driver_output2(port, &ch, 1, rbuf, 4);
}

static void driver_ok(ErlDrvPort port)
{
    char ch = XX_OK;
    driver_output2(port, &ch, 1, NULL, 0);
}

static void driver_eof(ErlDrvPort port)
{
    char ch = XX_REOF;
    driver_output2(port, &ch, 1, NULL, 0);
}


static void FILE_from_erlangv(ErlDrvData drv_data,  ErlIOVec* ev)
{
    Desc *desc = (Desc*) drv_data;
    SysIOVec  *iov = ev->iov;
    ErlDrvBinary* bin;

    switch ((&iov[1])->iov_base[0]) {

    case XX_OPEN: {
	char buf[BUFSIZ];
	char file[BUFSIZ];  /* should be FILENAME_MAX */
	char flags[4];       /* at most someething like rb+ */
	char* src;
	char* dst;
	char* src_end;
	char* dst_end;
	int n;

	if (desc->fp != NULL) {
	    driver_error(desc->port, XX_EINVAL);
	    return;
	}

	/* play it safe ? */
	n = vec_to_buf(ev, buf, BUFSIZ);
	src = buf + 1;
	src_end = buf + n;

	/* get file name */
	dst = file;
	dst_end = dst + BUFSIZ;  /* make room for a '\0' */
	while((src < src_end) && (dst < dst_end) && (*src != '\0'))
	    *dst++ = *src++;
	if ((src == src_end) || (dst == dst_end)) {
	    driver_error(desc->port, XX_EINVAL);
	}
	*dst = *src++;
	/* get flags */
	dst = flags;
	dst_end = dst + 4;
	while((src < src_end) && (dst < dst_end) && (*src != '\0'))
	    *dst++ = *src++;	
	if (dst == dst_end) {
	    driver_error(desc->port, XX_EINVAL);
	    return;
	}
	*dst = '\0';

	if ((desc->fp = fopen(file, flags))==NULL) {
	    driver_error(desc->port, errno);
	    return;
	}
	driver_ok(desc->port);
	break;
    }

    case XX_WRITE: {
	int i;
	iov[1].iov_base++;
	iov[1].iov_len--;
	for(i=1; i<ev->vsize; i++) {
	    if (fwrite(iov[i].iov_base, 1, iov[i].iov_len, desc->fp) !=
		iov[i].iov_len) {
		driver_error(desc->port, errno);
		return;
	    }
	}
	driver_ok(desc->port);
	break;
    }

    case XX_READ: {
	char ch = XX_VALUE;
	int rval;
	int sz = get_int32((&iov[1])->iov_base+1);

	if ((bin = driver_alloc_binary(sz)) == NULL) {
	    driver_error(desc->port, -1);
	    return;
	}

	if ((rval = fread(bin->orig_bytes, 1, sz, desc->fp)) != sz) {
	    if (feof(desc->fp)) {
		if (rval == 0) {
		    driver_free_binary(bin);
		    driver_eof(desc->port);
		    return;
		}
		driver_output_binary(desc->port, &ch, 1,bin, 0, rval);
		driver_free_binary(bin);
		return;
	    }
	    driver_free_binary(bin);
	    driver_error(desc->port, errno);
	    return;
	}
	driver_output_binary(desc->port, &ch, 1,bin, 0, sz);
	driver_free_binary(bin);
	break;
    }

    case XX_SEEK: {
	int offs = get_int32((&iov[1])->iov_base+1);
	int w = (int) (&iov[1])->iov_base[5];
	int whence;
	switch (w) {
	case 1: whence = SEEK_SET; break;
	case 2: whence = SEEK_CUR; break;
	case 3: whence = SEEK_END; break;
	}
	if ((w = fseek(desc->fp, offs, whence)) != 0) {
	    driver_error(desc->port, errno);
	    return;
	}
	driver_ok(desc->port);
	return;
    }

    case XX_TELL: {
	int offs;
	if ((offs = ftell(desc->fp)) == -1) {
	    driver_error(desc->port, errno);
	    return;
	}
	driver_ret32(desc->port, offs);
	break;
    }

    case XX_TRUNCATE: {
        int fno;
        int offs;
	/* is this really safe? */
        if (fflush(desc->fp) != 0) {
	    driver_error(desc->port, errno);
	    return;
	}
	if ((offs = ftell(desc->fp)) == -1) {
	    driver_error(desc->port, errno);
	    return;
	}
        fno = fileno(desc->fp);
#ifdef WIN32
	if (SetEndOfFile((HANDLE)fno) !=  0) {
	    driver_error(desc->port, GetLastError());
	    return;
	}
#else
        if (ftruncate(fno, offs) == -1) {
	    driver_error(desc->port, errno);
	    return;
	}
#endif
	driver_ok(desc->port);
	return;
    }

    case XX_FLUSH:
	if (fflush(desc->fp) != 0)
	    driver_error(desc->port, errno);
	else
	    driver_ok(desc->port);
	break;

    case XX_OEOF:
	if (feof(desc->fp))
	    driver_ret32(desc->port, 1);
	else
	    driver_ret32(desc->port,0);
	break;

    case XX_ERROR:
	if (ferror(desc->fp))
	    driver_ret32(desc->port, 1);
	else
	    driver_ret32(desc->port,0);
	break;

    case XX_GETC: {
	int ch;
	if ((ch = getc(desc->fp)) == EOF) {
	    if (feof(desc->fp)) {
		driver_eof(desc->port);
		return;
	    }
	    driver_error(desc->port, errno);
	    return;
	}
	driver_ret32(desc->port, ch);
	break;
    }

    case XX_SET_LINEBUF_SIZE: {
	int sz = get_int32((&iov[1])->iov_base+1);
	desc->linebuf_size = sz;
	driver_ok(desc->port);
	break;
    }

    case XX_GETS:
    case XX_GETS2: {
	int rval;
	long cpos1, cpos2;
	char header;
	
	if ((bin = driver_alloc_binary(desc->linebuf_size)) == NULL) {
	    driver_error(desc->port, -1);
	    return;
	}

	if ((cpos1 = ftell(desc->fp)) == -1) {
	    driver_free_binary(bin);
	    driver_error(desc->port, errno);
	    return;
	}

	if ((fgets(bin->orig_bytes, desc->linebuf_size,
		   desc->fp)) == NULL) {
	    driver_free_binary(bin);
	    if (feof(desc->fp)) {
		driver_eof(desc->port);
		return;
	    }
	    driver_error(desc->port, errno);
	    return;
	}
	if ((cpos2 = ftell(desc->fp)) == -1) {
	    driver_free_binary(bin);
	    driver_error(desc->port, errno);
	    return;
	}
	rval = cpos2 - cpos1;

	if (bin->orig_bytes[rval-1] == '\n' &&
	    bin->orig_bytes[rval] == 0) {
	    header = XX_FLINE;
	    /* GETS keep newline, GETS2 remove newline */
	    rval = rval - ((&iov[1])->iov_base[0] == XX_GETS ? 0 : 1);
	}
	else
	    header = XX_NOLINE;
	driver_output_binary(desc->port, &header, 1,bin, 0, rval);
	driver_free_binary(bin);
	break;
    }

    case XX_UNGETC: {
	int ch = (&iov[1])->iov_base[1];
	if (ungetc(ch, desc->fp) == EOF)
	    driver_error(desc->port, errno);
	else
	    driver_ok(desc->port);
	break;
    }
    
    default:
#ifdef DEBUG
	fprintf(stderr, "Unknown opcode %c\n\r", ((&iov[1])->iov_base[0]));
#endif
	driver_error(desc->port, XX_EINVAL);
	break;
    }
	
	
}

static void FILE_finish()
{
#ifndef USE_STDIO
    /*
     * Make sure any remaining buffers are flushed (this is done on exit() by
     * the normal stdio).
     */
    bbio_cleanup();
#endif
}


/*
 * Initialize and return a driver entry struct
 */

DRIVER_INIT(FILE_drv)
{
    FILE_driver_entry.init         = NULL;   /* Not used */
    FILE_driver_entry.start        = FILE_start;
    FILE_driver_entry.stop         = FILE_stop;
    FILE_driver_entry.output       = NULL;
    FILE_driver_entry.ready_input  = NULL;
    FILE_driver_entry.ready_output = NULL;
    FILE_driver_entry.driver_name  = "FILE_drv";
    FILE_driver_entry.finish       = FILE_finish;
    FILE_driver_entry.outputv      = FILE_from_erlangv;
    return &FILE_driver_entry;
}

