#include <oci.h>

char oci_err[1000];

int logon(char *username, char *password, char *dbname);
void logoff();
void error_proc(dvoid *errhp, sword status);
int select_racip(char *table1,char *table2,char *file);



