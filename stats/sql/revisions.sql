-- countRevisionsForMultiplePages
SELECT count(*) AS count, rev_page AS id
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;

-- countMinorEditRevisionsForMultiplePages
SELECT count(*) AS count, rev_page AS id
FROM revision
WHERE rev_page IN (?) AND rev_minor_edit = 1
GROUP BY rev_page;

-- countDeletedRevisionsForMultiplePages
SELECT count(*) AS count, rev_page AS id
FROM revision
WHERE rev_page IN (?) AND rev_deleted = 1
GROUP BY rev_page;

-- countDistinctContributorsForMultiplePages
SELECT count(DISTINCT rev_user) AS count, rev_page AS id
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;

-- firstRevisionForMultiplePages
SELECT min(rev_timestamp) AS firstRevision, rev_page AS id
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;

-- revisionLengthStatsForMultiplePages
SELECT
  rev_page AS id,
  SUM(rev_len) AS sum,
  MAX(rev_len) AS max,
  MIN(rev_len) AS min,
  AVG(rev_len) AS mean,
  VARIANCE(rev_len) AS variance
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;
