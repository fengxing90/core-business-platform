char db_err_info[2000];
FILE *fplog;

int stringtrim(char *string);
int stringsplit(char *string, char split, int number, char newstr[][2000]);
int numtime(int *ntime,time_t btime);
int IsProcessExist(char *ProcName);
int file_size(char *filename);
int date_add(char *old_date, char *new_date, int num);

//for temp Ì½¿Õ
