#ifndef __DS_UTIL_H_
#define __DS_UTIL_H_

size_t str_len(const char *str);
char *str_append_const(char *, const char *);
char *str_append(char *, char *);

/*******************************************************
   Processing parameter lists
**********************************************************/
/*
   Process a ;-separated and possibly multi-line text and
   create a list of name=value pairs from each
   item which has a "name=value" pattern. Whitespaces are removed.
   Input is not modified. Space is allocated;
   Also, simple "name" or "name=" patterns are processed and
   returned with value=NULL.
*/
struct name_value_pair {
    char *name;
    char *value;
    struct name_value_pair *next;
};

struct name_value_pair *text_to_nv_pairs(const char *text);
void free_nv_pairs(struct name_value_pair *pairs);
char *alloc_sprintf(const char *fmt_str, ...);

#endif
