#include "com_upserve_uppend_blobs_NativeIO.h"
#include <stdio.h>
#include <sys/mman.h>   // posix_madvise, madvise
#include <errno.h>   // errno
#include <string.h>   // strerror

// TODO add Lucene license and changes
// https://github.com/apache/lucene-solr/tree/master/lucene/misc/src/java/org/apache/lucene/store

/*
 * Class:     com_upserve_uppend_blobs_NativeIO
 * Method:    madvise
 * Signature: (Ljava/nio/MappedByteBuffer;Lcom/upserve/uppend/blobs/NativeIO/Advice;)V
 */
JNIEXPORT void JNICALL Java_com_upserve_uppend_blobs_NativeIO_madvise (JNIEnv *env, jclass _ignore, jobject buffer, jobject advice) {
    char exBuffer[80];
    jclass class_npe = (*env)->FindClass(env, "java/lang/NullPointerException");
    jclass class_ioex = (*env)->FindClass(env, "java/io/IOException");
    jclass class_argEx = (*env)->FindClass(env, "java/lang/IllegalArgumentException");

    if ((*env)->IsSameObject(env, buffer, NULL)) {
        (*env)->ThrowNew(env, class_npe, "buffer argument can not be null");
        return;
    }

    if ((*env)->IsSameObject(env, advice, NULL)) {
        (*env)->ThrowNew(env, class_npe, "advice argument can not be null");
        return;
    }

    // Parse the advice and lookup POSIX value (See man page for madvise)
    jclass adviceClass = (*env)->GetObjectClass(env, advice);
    jfieldID fidNumber = (*env)->GetFieldID(env, adviceClass, "value", "I");
    jint intAdvice = (*env)->GetIntField(env, advice, fidNumber);

    int osAdvice;
    switch(intAdvice) {
    case 0:
        osAdvice = MADV_NORMAL;
        break;
    case 1:
        osAdvice = POSIX_MADV_SEQUENTIAL;
        break;
    case 2:
        osAdvice = POSIX_MADV_RANDOM;
        break;
    case 3:
        osAdvice = POSIX_MADV_WILLNEED;
        break;
    case 4:
        osAdvice = POSIX_MADV_DONTNEED;
        break;
    default:
        sprintf(exBuffer, "invalid advice value: '%d'", intAdvice);
        (*env)->ThrowNew(env, class_argEx, exBuffer);
        return ;
    }

    void *p = (*env)->GetDirectBufferAddress(env, buffer);
    if (p == NULL) {
        (*env)->ThrowNew(env, class_ioex, strerror(errno));
        return;
    }

    size_t size = (size_t) (*env)->GetDirectBufferCapacity(env, buffer);
    if (size <= 0) {
        (*env)->ThrowNew(env, class_ioex, strerror(errno));
        return;
    }

    int page = getpagesize();

    // round start down to start of page
    long long start = (long long) p;
    start = start & (~(page-1));

    // round end up to start of page
    long long end = start + size;
    end = (end + page-1)&(~(page-1));
    size = (end-start);

    //printf("\nDO madvise: page=%d p=0x%lx 0x%lx size=0x%lx\n", page, p, start, size);

    int result = madvise((void *) start, size, osAdvice);
    if (result != 0) {
        sprintf(exBuffer, "system madvice call failed: '%d'", result);
        (*env)->ThrowNew(env, class_ioex, exBuffer);
        return ;
    }
    return;
}
