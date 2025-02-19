SELECT
    personId AS 'personId:ID',
    countryXName AS 'countryXName:STRING',
    countryYName AS 'countryYName:STRING',
    creationDay AS 'startDate:DATE',
    2 + salt * 37 % 5 AS 'durationDays:INT'
FROM
    (SELECT
        personId,
        abs(frequency - (SELECT percentile_disc(0.55) WITHIN GROUP (ORDER BY frequency) FROM personNumFriends)) AS diff
    FROM personNumFriends
    ORDER BY diff, md5(personId)
    LIMIT 20
    ),
    (SELECT
        countryName AS countryXName,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.77) WITHIN GROUP (ORDER BY frequency) FROM countryNumPersons)) AS diff
    FROM countryNumPersons
    ORDER BY diff, countryName
    LIMIT 10
    ),
    (SELECT
        countryName AS countryYName,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.66) WITHIN GROUP (ORDER BY frequency) FROM countryNumPersons)) AS diff
    FROM countryNumPersons
    ORDER BY diff, countryName
    LIMIT 10
    ),
    (SELECT
        creationDay,
        frequency AS freq,
        abs(frequency - (SELECT percentile_disc(0.15) WITHIN GROUP (ORDER BY frequency) FROM creationDayNumMessages)) AS diff
    FROM creationDayNumMessages
    ORDER BY diff, creationDay
    LIMIT 15
    ),
    (SELECT unnest(generate_series(1, 20)) AS salt)
WHERE countryXName != countryYName
ORDER BY md5(concat(personId, countryXName, countryYName, creationDay, salt))
LIMIT 500
