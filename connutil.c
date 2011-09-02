#include "openssl/hmac.h"

#include "postgres.h"
#include "lib/stringinfo.h"

#include "connutil.h"

static char *sign_by_secretkey(char *input, char *secretkey);
static int b64_encode(const uint8 *src, unsigned len, uint8 *dst);

/*
 * Constructs GMT-style string
 */
char *
httpdate(time_t *timer)
{
	char   *datestring;
	time_t t;
	struct tm *gt;

	t = time(timer);
	gt = gmtime(&t);
	datestring = (char *) palloc0(256 * sizeof(char));
	strftime(datestring, 256 * sizeof(char), "%a, %d %b %Y %H:%M:%S +0000", gt);
	return datestring;
}

/*
 * Construct signed string for the Authorization header,
 * following the Amazon S3 REST API spec.
 */
char *
s3_signature(char *method, char *datestring,
			 char *bucket, char *file, char *secretkey)
{
	size_t		rs_size;
	char	   *resource;
	StringInfoData buf;

	rs_size = strlen(bucket) + strlen(file) + 3; /* 3 = '/' + '/' + '\0' */
	resource = (char *) palloc0(rs_size);

	snprintf(resource, rs_size, "/%s/%s", bucket, file);
	initStringInfo(&buf);
	/*
	 * StringToSign = HTTP-Verb + "\n" +
	 * 		Content-MD5 + "\n" +
	 * 		Content-Type + "\n" +
	 * 		Date + "\n" +
	 * 		CanonicalizedAmzHeaders +
	 * 		CanonicalizedResource;
	 */
	appendStringInfo(&buf, "%s\n", method);
	appendStringInfo(&buf, "\n");
	appendStringInfo(&buf, "\n");
	appendStringInfo(&buf, "%s\n", datestring);
//	appendStringInfo(&buf, "");
	appendStringInfo(&buf, "%s", resource);

//elog(INFO, "StringToSign:%s", buf.data);
	return sign_by_secretkey(buf.data, secretkey);
}

static char *
sign_by_secretkey(char *input, char *secretkey)
{
	HMAC_CTX ctx;
	/* sha1 has to be 30 charcters */
	char	result[256];
	unsigned int	len;
	/* base64 may enlarge the size up to double */
	char	b64_result[256];
	int		b64_len;

	HMAC_CTX_init(&ctx);
	HMAC_Init(&ctx, secretkey, strlen(secretkey), EVP_sha1());
	HMAC_Update(&ctx, (unsigned char *) input, strlen(input));
	HMAC_Final(&ctx, (unsigned char *) result, &len);
	HMAC_CTX_cleanup(&ctx);

	b64_len = b64_encode((unsigned char *) result, len, (unsigned char *) b64_result);
	b64_result[b64_len] = '\0';

	return pstrdup(b64_result);
}

/*
 * BASE64 - duplicated :(
 */

static int
b64_encode(const uint8 *src, unsigned len, uint8 *dst)
{
	static const unsigned char _base64[] =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

	uint8	   *p,
			   *lend = dst + 76;
	const uint8 *s,
			   *end = src + len;
	int			pos = 2;
	unsigned long buf = 0;

	s = src;
	p = dst;

	while (s < end)
	{
		buf |= *s << (pos << 3);
		pos--;
		s++;

		/*
		 * write it out
		 */
		if (pos < 0)
		{
			*p++ = _base64[(buf >> 18) & 0x3f];
			*p++ = _base64[(buf >> 12) & 0x3f];
			*p++ = _base64[(buf >> 6) & 0x3f];
			*p++ = _base64[buf & 0x3f];

			pos = 2;
			buf = 0;
		}
		if (p >= lend)
		{
			*p++ = '\n';
			lend = p + 76;
		}
	}
	if (pos != 2)
	{
		*p++ = _base64[(buf >> 18) & 0x3f];
		*p++ = _base64[(buf >> 12) & 0x3f];
		*p++ = (pos == 0) ? _base64[(buf >> 6) & 0x3f] : '=';
		*p++ = '=';
	}

	return p - dst;
}
