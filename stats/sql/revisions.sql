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

-- countDistinctContributorsForMultiplePages
SELECT count(DISTINCT rev_user) AS count, rev_page AS id
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;

-- firstRevisionForMultiplePages
SELECT
  min(rev_timestamp) AS firstRevision,
  rev_len AS bytes,
  rev_page AS id
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;

-- revisionLengthStatsForMultiplePages
SELECT
  rev_page AS id,
  sum(rev_len) AS sum,
  max(rev_len) AS max,
  min(rev_len) AS min,
  avg(rev_len) AS mean,
  variance(rev_len) AS variance
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;

-- revisionStatsForMultiplePages
SET @last := 0;
SET @last_page := NULL;
SELECT
  context.rev_page AS id,
  sum(IF(sign(context.diff) = 1, 1, 0)) AS additionCount,
  sum(IF(sign(context.diff) = -1, 1, 0)) AS deletionCount,
  sum(IF(sign(context.diff) = 1, context.diff, 0)) AS additionSum,
  sum(IF(sign(context.diff) = -1, abs(context.diff), 0)) AS deletionSum,
  max(IF(sign(context.diff) = 1, context.diff, 0)) AS additionMax,
  max(IF(sign(context.diff) = -1, abs(context.diff), 0)) AS deletionMax,
  variance(IF(sign(context.diff) = 1, context.diff, 0)) AS additionVariance,
  variance(IF(sign(context.diff) = -1, abs(context.diff), 0)) AS deletionVariance
FROM (
  SELECT
    rev_page,
    IF(
      @last_page IS NULL,
      (@last_page := rev_page),
      IF(
        @last_page <> rev_page,
        (@last_page := NULL),
        @last_page
      )
    ) AS last_page,
    IF(
      @last_page = rev_page,
      CAST(rev_len AS SIGNED) - COALESCE(@last, 0),
      NULL
    ) AS diff,
    (@last := CAST(rev_len AS SIGNED)) AS last
  FROM revision
  WHERE rev_page IN (?)
  ORDER BY rev_page, rev_timestamp
) AS context
GROUP BY context.rev_page;
