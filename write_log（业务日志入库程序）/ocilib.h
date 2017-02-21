#include <oci.h>

char oci_err[1000];

int logon(char *username, char *password, char *dbname);
void logoff();
void error_proc(dvoid *errhp, sword status);
int  insert_detail_log(char *table,char *temp1,char *temp2,char *temp3,char *temp4,char *temp5,char *temp6);
int insert_collect_log(char *table,char *temp1,char *temp2,char *temp3,char *temp4);
int select_business(char *table,char *configpath);



