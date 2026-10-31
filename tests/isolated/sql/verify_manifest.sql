-- APPROVAL REQUIRED: tiny isolated tickit_it database ONLY. Never production.
-- Stop if either identity differs from the approved test database/loader user.
SELECT current_database(), current_user;

-- Initial fixture: category=1 venue=1 users=2 event=1 listing=1 sales=1.
SELECT 'category' AS dataset, COUNT(*) AS rows FROM stage.category
UNION ALL SELECT 'venue', COUNT(*) FROM stage.venue
UNION ALL SELECT 'users', COUNT(*) FROM stage.users
UNION ALL SELECT 'event', COUNT(*) FROM stage.event
UNION ALL SELECT 'listing', COUNT(*) FROM stage.listing
UNION ALL SELECT 'sales', COUNT(*) FROM stage.sales;
-- Initial fixture: users=2 event=1 sales=1 listing=1 receipts=1.
SELECT 'users' AS dataset, COUNT(*) AS rows FROM dim.users
UNION ALL SELECT 'event', COUNT(*) FROM dim.event
UNION ALL SELECT 'sales', COUNT(*) FROM fact.sales
UNION ALL SELECT 'listing', COUNT(*) FROM fact.listing
UNION ALL SELECT 'receipts', COUNT(*) FROM control.batch_commits;

-- All queries below must return zero rows.
SELECT salesid,COUNT(*) FROM fact.sales GROUP BY salesid HAVING COUNT(*)<>1;
SELECT listid,COUNT(*) FROM fact.listing GROUP BY listid HAVING COUNT(*)<>1;
SELECT batch_id,COUNT(*) FROM control.batch_commits GROUP BY batch_id HAVING COUNT(*)<>1;
SELECT userid FROM dim.users GROUP BY userid HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END)<>1;
SELECT eventid FROM dim.event GROUP BY eventid HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END)<>1;
SELECT a.userid,a.user_key,b.user_key FROM dim.users a JOIN dim.users b
 ON a.userid=b.userid AND a.user_key<b.user_key
 AND a.effective_date<b.expiry_date AND b.effective_date<a.expiry_date;
SELECT a.eventid,a.event_key,b.event_key FROM dim.event a JOIN dim.event b
 ON a.eventid=b.eventid AND a.event_key<b.event_key
 AND a.effective_date<b.expiry_date AND b.effective_date<a.expiry_date;
SELECT f.salesid FROM fact.sales f
 LEFT JOIN stage.sales t ON t.salesid=f.salesid
 LEFT JOIN dim.users s ON s.user_key=f.seller_key AND s.userid=t.sellerid
 LEFT JOIN dim.users b ON b.user_key=f.buyer_key AND b.userid=t.buyerid
 LEFT JOIN dim.event e ON e.event_key=f.event_key AND e.eventid=f.eventid
 WHERE t.salesid IS NULL OR s.user_key IS NULL OR b.user_key IS NULL OR e.event_key IS NULL
 OR f.saletime<s.effective_date::TIMESTAMP OR f.saletime>=s.expiry_date::TIMESTAMP
 OR f.saletime<b.effective_date::TIMESTAMP OR f.saletime>=b.expiry_date::TIMESTAMP
 OR f.saletime<e.effective_date::TIMESTAMP OR f.saletime>=e.expiry_date::TIMESTAMP;
SELECT f.listid FROM fact.listing f
 LEFT JOIN stage.listing t ON t.listid=f.listid
 LEFT JOIN dim.users s ON s.user_key=f.seller_key AND s.userid=t.sellerid
 LEFT JOIN dim.event e ON e.event_key=f.event_key AND e.eventid=f.eventid
 WHERE t.listid IS NULL OR s.user_key IS NULL OR e.event_key IS NULL
 OR f.listtime<s.effective_date::TIMESTAMP OR f.listtime>=s.expiry_date::TIMESTAMP
 OR f.listtime<e.effective_date::TIMESTAMP OR f.listtime>=e.expiry_date::TIMESTAMP;

-- Save before/after duplicate delivery: same receipt and counts; no new Glue run.
SELECT batch_id,input_sha,output_sha,attempt,fence_epoch,load_date,committed_at
FROM control.batch_commits ORDER BY batch_id;
