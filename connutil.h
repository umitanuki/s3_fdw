#ifndef _S3_CONNUTIL_H_
#define _S3_CONNUTIL_H_

#include <time.h>

extern char *httpdate(time_t *timer);
extern char *s3_signature(char *method, char *datestring,
						  char *bucket, char *file, char *secretkey);


#endif /* _S3_CONNUTIL_H */
