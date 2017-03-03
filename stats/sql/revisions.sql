-- countRevisions
SELECT count(*) AS count FROM revision WHERE rev_page = ?;

-- countRevisionsForMultiplePages
SELECT count(*) AS count FROM revision WHERE rev_page IN ?;
