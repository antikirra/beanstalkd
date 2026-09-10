char *ctdir(void);
void  ctfail(void);
// abort()s: telling the compiler so lets every assert() below act as
// a real narrowing guard. Without it a pointer checked by assert is
// still "possibly NULL" afterwards, and -Warray-bounds /
// -Wstringop-overflow read that as a zero-sized destination.
void  ctfailnow(void) __attribute__((noreturn));
void  ctresettimer(void);
void  ctstarttimer(void);
void  ctstoptimer(void);
void  ctsetbytes(int);
void  ctlogpn(const char*, int, const char*, ...) __attribute__((format(printf, 3, 4)));
#define ctlog(...) ctlogpn(__FILE__, __LINE__, __VA_ARGS__)
#define assert(x) do if (!(x)) {\
	ctlog("%s", "test: " #x);\
	ctfailnow();\
} while (0)
#define assertf(x, ...) do if (!(x)) {\
	ctlog("%s", "test: " #x);\
	ctlog(__VA_ARGS__);\
	ctfailnow();\
} while (0)
