SELECT
    personId AS 'personId:ID',
    creationDay AS 'startDate:DATE',
    2 + salt * 37 % 5 AS 'durationDays:INT'
FROM
    (SELECT
        personId,
        abs(frequency - (SELECT percentile_disc(0.60) WITHIN GROUP (ORDER BY frequency) FROM personNumFriends)) AS diff
    FROM personNumFriends
    ORDER BY diff, md5(personId)
    LIMIT 10
    ),
    (SELECT
        creationDay,
        abs(frequency - (SELECT percentile_disc(0.65) WITHIN GROUP (ORDER BY frequency) FROM creationDayNumMessages)) AS diff
    FROM creationDayNumMessages
    ORDER BY diff, md5(creationDay)
    LIMIT 10
    ),
    (SELECT unnest(generate_series(1, 20)) AS salt)
ORDER BY md5(concat(personId, creationDay, salt))
LIMIT 500
