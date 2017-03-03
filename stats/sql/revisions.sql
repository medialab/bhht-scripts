-- countRevisions
SELECT count(*) AS count FROM revision WHERE rev_page = ?;
