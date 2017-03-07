# Stats: What to retrieve?

## Schema

Structure of the `revision` table:

```
+--------------------+---------------------+------+-----+---------+-------+
| Field              | Type                | Null | Key | Default | Extra |
+--------------------+---------------------+------+-----+---------+-------+
| rev_id             | int(8) unsigned     | NO   |     | 0       |       |
| rev_page           | int(8) unsigned     | NO   |     | 0       |       |
| rev_text_id        | bigint(10) unsigned | YES  |     | NULL    |       |
| rev_comment        | varbinary(255)      | YES  |     | NULL    |       |
| rev_user           | bigint(10) unsigned | YES  |     | NULL    |       |
| rev_user_text      | varbinary(255)      | YES  |     | NULL    |       |
| rev_timestamp      | varbinary(14)       | NO   |     |         |       |
| rev_minor_edit     | tinyint(1) unsigned | NO   |     | 0       |       |
| rev_deleted        | tinyint(1) unsigned | NO   |     | 0       |       |
| rev_len            | bigint(10) unsigned | YES  |     | NULL    |       |
| rev_parent_id      | int(8) unsigned     | YES  |     | NULL    |       |
| rev_sha1           | varbinary(32)       | YES  |     | NULL    |       |
| rev_content_model  | varbinary(32)       | YES  |     | NULL    |       |
| rev_content_format | varbinary(64)       | YES  |     | NULL    |       |
+--------------------+---------------------+------+-----+---------+-------+
```

## Notes

* `rev_len` tracks the byte length of the page after revision's application.

## Queries

* Number of revisions per page
* Number of `minor_edit` revisions per page
* Number of distinct contributors per page
* Date of page's first revision
* Number of bytes at creation
* Number of +/- diffs per page
* Sum of +/- diffs per page
* Max of +/- diffs per page
