#include <stdio.h>

void example2_sendString(const char* pszVal)
{
	// note: printfs called from C won't be
	// flushed to stdout until the Java
	// process completes
	printf("(C) '%s'\n", pszVal);
}

void example2_getString(char** ppszVal)
{
	*ppszVal = (char*)malloc(sizeof(char) * 6);
	memset(*ppszVal, 0, sizeof(char) * 6);
	strcpy(*ppszVal, "hello");
}

void example2_cleanup(char* pszVal)
{
	free(pszVal);
}
