-- countRevisions
SELECT count(*) AS count FROM revision WHERE rev_page = ?;

-- countRevisionsForMultiplePages
SELECT count(*) AS count, rev_page AS id
FROM revision
WHERE rev_page IN (?)
GROUP BY rev_page;
