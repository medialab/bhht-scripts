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
SELECT count(DISTINCT rev_user) as count, rev_page AS id
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;

-- firstRevisionForMultiplePages
SELECT min(rev_timestamp) as firstRevision, rev_page AS id
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;

-- revisionLengthStatsForMultiplePages
SELECT
  SUM(rev_len) as sum,
  MAX(rev_len) as max,
  MIN(rev_len) as min,
  AVG(rev_len) as mean,
  VARIANCE(rev_len) as variance
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;
