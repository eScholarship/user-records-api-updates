SELECT
    u.[Proprietary ID],
    u.[ID] AS 'User ID',
    ur.[Data Source Proprietary ID]
FROM [User] u
    JOIN [User Record] ur
        ON u.ID = ur.[User ID]
        AND ur.[Data Source] = 'Manual'
WHERE
    u.[Proprietary ID] IN (
    -- REPLACE
    );