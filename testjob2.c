#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static Tube *dtube;

void
cttest_job_copy_preserves_body()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *j = make_job(42, 0, 1000000000, 6, dtube);
    assertf(j != NULL, "make_job");
    memcpy(j->body, "hello\n", 6);
    j->r.state = Ready;

    Job *c = job_copy(j);
    assertf(c != NULL, "job_copy must not return NULL");
    assertf(c->r.pri == 42, "priority must be preserved");
    assertf(c->r.body_size == 6, "body_size must be preserved");
    assertf(memcmp(c->body, "hello\n", 6) == 0, "body must be copied");
    assertf(c->r.state == Copy, "copy state must be Copy");
    assertf(c->tube == j->tube, "tube pointer must match");
    assertf(c->file == NULL, "copy must not reference WAL file");

    /* copy must be independently freeable */
    job_free(c);
    job_free(j);
}

void
cttest_job_copy_null_input()
{
    Job *c = job_copy(NULL);
    assertf(c == NULL, "job_copy(NULL) must return NULL");
}

void
cttest_job_state_names()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *j = make_job(1, 0, 1000000000, 0, dtube);

    j->r.state = Ready;
    assertf(strcmp(job_state(j), "ready") == 0, "ready");

    j->r.state = Reserved;
    assertf(strcmp(job_state(j), "reserved") == 0, "reserved");

    j->r.state = Buried;
    assertf(strcmp(job_state(j), "buried") == 0, "buried");

    j->r.state = Delayed;
    assertf(strcmp(job_state(j), "delayed") == 0, "delayed");

    j->r.state = Invalid;
    assertf(strcmp(job_state(j), "invalid") == 0, "invalid");

    job_free(j);
}

void
cttest_job_list_insert_remove()
{
    TUBE_ASSIGN(dtube, make_tube("default"));

    Job head;
    job_list_reset(&head);
    assertf(job_list_is_empty(&head), "new list must be empty");

    Job *a = make_job(1, 0, 1000000000, 0, dtube);
    Job *b = make_job(2, 0, 1000000000, 0, dtube);

    job_list_insert(&head, a);
    assertf(!job_list_is_empty(&head), "list must not be empty after insert");

    job_list_insert(&head, b);

    /* remove a */
    Job *got = job_list_remove(a);
    assertf(got == a, "must return removed job");
    assertf(!job_list_is_empty(&head), "b still in list");

    /* remove b */
    got = job_list_remove(b);
    assertf(got == b, "must return b");
    assertf(job_list_is_empty(&head), "list must be empty now");

    /* double remove must return NULL */
    got = job_list_remove(a);
    assertf(got == NULL, "double remove must return NULL");

    job_free(a);
    job_free(b);
}

void
cttest_job_list_remove_null()
{
    Job *got = job_list_remove(NULL);
    assertf(got == NULL, "remove NULL must return NULL");
}

void
cttest_job_find_nonexistent()
{
    Job *j = job_find(999999999);
    assertf(j == NULL, "must not find nonexistent job");
}

void
cttest_job_pri_equal_breaks_tie_by_id()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *a = make_job(100, 0, 1000000000, 0, dtube);
    Job *b = make_job(100, 0, 1000000000, 0, dtube);

    /* same priority — lower id wins */
    assertf(a->r.id < b->r.id, "a must have lower id");
    assertf(job_pri_less(a, b), "a must come before b (lower id)");
    assertf(!job_pri_less(b, a), "b must not come before a");

    job_free(a);
    job_free(b);
}

void
cttest_job_delay_less()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *a = make_job(1, 0, 1000000000, 0, dtube);
    Job *b = make_job(1, 0, 1000000000, 0, dtube);

    a->r.deadline_at = 1000;
    b->r.deadline_at = 2000;
    assertf(job_delay_less(a, b), "earlier deadline must be less");
    assertf(!job_delay_less(b, a), "later deadline must not be less");

    /* equal deadlines — lower id wins */
    b->r.deadline_at = 1000;
    assertf(job_delay_less(a, b), "same deadline, lower id wins");

    job_free(a);
    job_free(b);
}
