PRINT '--- SQL Transformation Script (Staging -> Final + Views) --- START ---';
GO

BEGIN TRANSACTION;

BEGIN TRY

    PRINT '[Cleanup] Dropping existing Views...';
    DROP VIEW IF EXISTS dbo.vw_FraudulentTransactions;
    DROP VIEW IF EXISTS dbo.vw_VipUsers;
    DROP VIEW IF EXISTS dbo.vw_BlockedEntities;
    DROP VIEW IF EXISTS dbo.vw_DailyTransactionSummary;
    PRINT '[Cleanup] Views dropped (if they existed).';

    PRINT '[Cleanup] Dropping existing Foreign Keys on FINAL tables...';
    IF OBJECT_ID('dbo.Logs', 'U') IS NOT NULL AND OBJECT_ID('dbo.FK_Logs_Transactions', 'F') IS NOT NULL ALTER TABLE [dbo].[Logs] DROP CONSTRAINT [FK_Logs_Transactions];
    IF OBJECT_ID('dbo.ScheduledPayments', 'U') IS NOT NULL AND OBJECT_ID('dbo.FK_ScheduledPayments_Cards', 'F') IS NOT NULL ALTER TABLE [dbo].[ScheduledPayments] DROP CONSTRAINT [FK_ScheduledPayments_Cards];
    IF OBJECT_ID('dbo.Transactions', 'U') IS NOT NULL AND OBJECT_ID('dbo.FK_Transactions_Cards_To', 'F') IS NOT NULL ALTER TABLE [dbo].[Transactions] DROP CONSTRAINT [FK_Transactions_Cards_To];
    IF OBJECT_ID('dbo.Transactions', 'U') IS NOT NULL AND OBJECT_ID('dbo.FK_Transactions_Cards_From', 'F') IS NOT NULL ALTER TABLE [dbo].[Transactions] DROP CONSTRAINT [FK_Transactions_Cards_From];
    IF OBJECT_ID('dbo.Cards', 'U') IS NOT NULL AND OBJECT_ID('dbo.FK_Cards_Users', 'F') IS NOT NULL ALTER TABLE [dbo].[Cards] DROP CONSTRAINT [FK_Cards_Users];
    PRINT '[Cleanup] Foreign Keys dropped (if they existed).';

    PRINT '[Cleanup] Dropping existing FINAL tables...';
    DROP TABLE IF EXISTS [dbo].[Logs];
    DROP TABLE IF EXISTS [dbo].[ScheduledPayments];
    DROP TABLE IF EXISTS [dbo].[Transactions];
    DROP TABLE IF EXISTS [dbo].[Reports];
    DROP TABLE IF EXISTS [dbo].[Cards];
    DROP TABLE IF EXISTS [dbo].[Users];
    PRINT '[Cleanup] FINAL tables dropped (if they existed).';


    PRINT '[Create & Load] Processing FINAL table: Users from stg_Users...';
    CREATE TABLE [dbo].[Users] (
        [UserID] BIGINT PRIMARY KEY,
        [FullName] NVARCHAR(MAX) NULL,
        [PhoneNumber] NVARCHAR(50) NULL,
        [EmailAddress] NVARCHAR(255) NULL,
        [RegistrationDate] DATETIME2 NULL,
        [LastActivityDate] DATETIME2 NULL,
        [IsVIP] BIT NOT NULL DEFAULT 0,
        [TotalBalance] FLOAT NOT NULL DEFAULT 
        [IsEmailValid] BIT NULL,
        [IsPhoneValid] BIT NULL
    );
    PRINT '  Table Users created with new column names.';

WITH UsersCTE AS (
        SELECT
            TRY_CAST(s.id AS BIGINT) AS src_id, -- <<< Manba: id
            TRIM(s.name) AS src_name, -- <<< Manba: name
            TRIM(CAST(s.phone_number AS NVARCHAR(50))) AS src_phone, -- <<< Manba: phone_number
            TRIM(LOWER(s.email)) AS src_email, -- <<< Manba: email
            TRY_CAST(s.created_at AS DATETIME2) AS src_created, -- <<< Manba: created_at
            TRY_CAST(s.last_active_at AS DATETIME2) AS src_last_active, -- <<< Manba: last_active_at
            CASE WHEN LOWER(TRIM(CAST(s.is_vip AS VARCHAR(10)))) IN ('true', '1', 'yes') THEN 1 ELSE 0 END AS src_is_vip, -- <<< Manba: is_vip
            ISNULL(TRY_CAST(s.total_balance AS FLOAT), 0) AS src_balance, -- <<< Manba: total_balance
            CASE WHEN s.email LIKE '%_@__%.__%' AND s.email NOT LIKE '% %' THEN 1 ELSE 0 END AS email_valid,
            CASE WHEN TRY_CAST(s.phone_number AS VARCHAR(50)) LIKE '%[0-9][0-9][0-9][0-9][0-9][0-9]%' THEN 1 ELSE 0 END AS phone_valid,
            ROW_NUMBER() OVER(PARTITION BY TRY_CAST(s.id AS BIGINT) ORDER BY (SELECT NULL)) as rn
        FROM dbo.stg_Users s -- <<< Manba Jadval: stg_Users
        WHERE TRY_CAST(s.id AS BIGINT) IS NOT NULL
    )
    INSERT INTO [dbo].[Users] (
        [UserID], [FullName], [PhoneNumber], [EmailAddress], [RegistrationDate], [LastActivityDate],
        [IsVIP], [TotalBalance], [IsEmailValid], [IsPhoneValid]
    )
    SELECT
        src_id, src_name, src_phone, src_email, src_created, src_last_active,
        src_is_vip, src_balance, email_valid, phone_valid
    FROM UsersCTE WHERE rn = 1;
    PRINT '  Data inserted into Users (transformed).';

    -- --- 1.2 Cards ---
    PRINT '[Create & Load] Processing FINAL table: Cards from stg_Cards...';
    CREATE TABLE [dbo].[Cards] (
        [CardID] BIGINT PRIMARY KEY, -- <<< Final Ustun Nomi: CardID (PK)
        [UserID] BIGINT NULL, -- <<< Final Ustun Nomi: UserID (FK uchun)
        [CardNumber] NVARCHAR(50) NULL UNIQUE, -- <<< Final Ustun Nomi: CardNumber (Unique)
        [CurrentBalance] FLOAT NOT NULL DEFAULT 0, -- <<< Final Ustun Nomi: CurrentBalance
        [IssueDate] DATETIME2 NULL, -- <<< Final Ustun Nomi: IssueDate
        [CardType] NVARCHAR(MAX) NULL, -- <<< Final Ustun Nomi: CardType
        [CreditLimit] FLOAT NOT NULL DEFAULT 0, -- <<< Final Ustun Nomi: CreditLimit
        [CardStatus] NVARCHAR(50) NULL -- <<< Final Ustun Nomi: CardStatus
    );
    PRINT '  Table Cards created.';

WITH CardsCTE AS (
        SELECT
            TRY_CAST(s.id AS BIGINT) as src_id, -- <<< Manba: id
            TRY_CAST(s.user_id AS BIGINT) as src_user_id, -- <<< Manba: user_id
            TRIM(CAST(s.card_number AS NVARCHAR(50))) as src_card_num, -- <<< Manba: card_number
            ISNULL(TRY_CAST(s.balance AS FLOAT), 0) as src_balance, -- <<< Manba: balance
            TRY_CAST(s.created_at AS DATETIME2) as src_created, -- <<< Manba: created_at
            TRIM(s.card_type) as src_card_type, -- <<< Manba: card_type
            ISNULL(TRY_CAST(s.limit_amount AS FLOAT), 0) as src_limit, -- <<< Manba: limit_amount
            TRIM(LOWER(CAST(s.status AS NVARCHAR(50)))) as src_status, -- <<< Manba: status
            ROW_NUMBER() OVER(PARTITION BY TRY_CAST(s.id AS BIGINT) ORDER BY (SELECT NULL)) as rn_id,
            ROW_NUMBER() OVER(PARTITION BY TRIM(CAST(s.card_number AS NVARCHAR(50))) ORDER BY TRY_CAST(s.id AS BIGINT)) as rn_card_num
        FROM dbo.stg_Cards s -- <<< Manba Jadval: stg_Cards
        WHERE TRY_CAST(s.id AS BIGINT) IS NOT NULL AND TRY_CAST(s.user_id AS BIGINT) IS NOT NULL AND TRIM(CAST(s.card_number AS NVARCHAR(50))) IS NOT NULL
    )
    INSERT INTO [dbo].[Cards] ( -- <<< Maqsad Ustun Nomlari
        [CardID], [UserID], [CardNumber], [CurrentBalance], [IssueDate], [CardType], [CreditLimit], [CardStatus]
    )
    SELECT -- <<< Manba CTE dagi nomlar
        src_id, src_user_id, src_card_num, src_balance, src_created, src_card_type, src_limit, src_status
    FROM CardsCTE WHERE rn_id = 1 AND rn_card_num = 1;
    PRINT '  Data inserted into Cards (transformed).';

    -- --- 1.3 Transactions ---
    PRINT '[Create & Load] Processing FINAL table: Transactions from stg_Transactions...';
    CREATE TABLE [dbo].[Transactions] (
        [TransactionID] BIGINT PRIMARY KEY, -- <<< Final Ustun Nomi: TransactionID (PK)
        [FromCardID] BIGINT NULL, -- <<< Final Ustun Nomi: FromCardID (FK uchun)
        [ToCardID] BIGINT NULL, -- <<< Final Ustun Nomi: ToCardID (FK uchun)
        [TransactionAmount] FLOAT NOT NULL DEFAULT 0, -- <<< Final Ustun Nomi: TransactionAmount
        [TransactionStatus] NVARCHAR(50) NULL, -- <<< Final Ustun Nomi: TransactionStatus
        [TransactionTimestamp] DATETIME2 NULL, -- <<< Final Ustun Nomi: TransactionTimestamp
        [TransactionType] NVARCHAR(MAX) NULL, -- <<< Final Ustun Nomi: TransactionType
        [ExceedsTransactionThreshold] BIT NOT NULL DEFAULT 0,
        [ExceedsCardLimit] BIT NOT NULL DEFAULT 0,
        [CardLimitCompared] FLOAT NULL
    );
    PRINT '  Table Transactions created.';

WITH TransactionsCTE AS (
        SELECT
            TRY_CAST(s.id AS BIGINT) as src_id, -- <<< Manba: id
            TRY_CAST(s.from_card_id AS BIGINT) as src_from_card, -- <<< Manba: from_card_id
            TRY_CAST(s.to_card_id AS BIGINT) as src_to_card, -- <<< Manba: to_card_id
            ISNULL(TRY_CAST(s.amount AS FLOAT), 0) as src_amount, -- <<< Manba: amount
            TRIM(LOWER(CAST(s.status AS NVARCHAR(50)))) as src_status, -- <<< Manba: status
            TRY_CAST(s.created_at AS DATETIME2) as src_created, -- <<< Manba: created_at
            TRIM(s.transaction_type) as src_type, -- <<< Manba: transaction_type
            CASE WHEN ISNULL(TRY_CAST(s.amount AS FLOAT), 0) > 15000 THEN 1 ELSE 0 END AS flag_threshold,
            ISNULL(c.CreditLimit, 0) AS card_limit_val, -- <<< Staging Cards dagi limit (maqsad nomi bilan)
            CASE WHEN ISNULL(TRY_CAST(s.amount AS FLOAT), 0) > ISNULL(c.CreditLimit, 0) AND ISNULL(c.CreditLimit, 0) > 0 THEN 1 ELSE 0 END AS flag_limit,
            ROW_NUMBER() OVER(PARTITION BY TRY_CAST(s.id AS BIGINT) ORDER BY (SELECT NULL)) as rn
        FROM dbo.stg_Transactions s -- <<< Manba Jadval: stg_Transactions
        LEFT JOIN dbo.Cards c ON TRY_CAST(s.from_card_id AS BIGINT) = c.CardID -- <<< JOIN FINAL Cards bilan (CardID bo'yicha) ??? TEKSHIRING!
        WHERE TRY_CAST(s.id AS BIGINT) IS NOT NULL AND TRY_CAST(s.amount AS FLOAT) IS NOT NULL AND TRY_CAST(s.created_at AS DATETIME2) IS NOT NULL
    )
    INSERT INTO [dbo].[Transactions] ( -- <<< Maqsad Ustun Nomlari
        [TransactionID], [FromCardID], [ToCardID], [TransactionAmount], [TransactionStatus], [TransactionTimestamp], [TransactionType],
        [ExceedsTransactionThreshold], [ExceedsCardLimit], [CardLimitCompared]
    )
    SELECT -- <<< Manba CTE dagi nomlar
        src_id, src_from_card, src_to_card, src_amount, src_status, src_created, src_type,
        flag_threshold, flag_limit, card_limit_val
    FROM TransactionsCTE WHERE rn = 1;
    PRINT '  Data inserted into Transactions (transformed).';

    -- --- 1.4 Logs, 1.5 Reports, 1.6 ScheduledPayments (Shu kabi davom eting) ---
    -- Har bir jadval uchun CREATE TABLE (final nomlar bilan) va INSERT INTO SELECT (manba nomlar bilan, transformatsiya qilib) yozing.
    -- Misol uchun Logs:
    PRINT '[Create & Load] Processing FINAL table: Logs from stg_Logs...';
    CREATE TABLE [dbo].[Logs] ([LogEntryID] BIGINT PRIMARY KEY, [RelatedTransactionID] BIGINT, [Message] NVARCHAR(MAX), [LogTimestamp] DATETIME2, [LogLevel] NVARCHAR(50));
    PRINT '  Table Logs created.';
    WITH LogsCTE AS (SELECT TRY_CAST(id as BIGINT) as src_id, TRY_CAST(transaction_id as BIGINT) as src_tx_id, log_message as src_msg, TRY_CAST(created_at as DATETIME2) as src_ts, log_level as src_lvl, ROW_NUMBER() OVER(PARTITION BY id ORDER BY(SELECT NULL)) as rn FROM stg_Logs WHERE TRY_CAST(id as BIGINT) IS NOT NULL)
    INSERT INTO [dbo].[Logs] ([LogEntryID], [RelatedTransactionID], [Message], [LogTimestamp], [LogLevel]) SELECT src_id, src_tx_id, src_msg, src_ts, src_lvl FROM LogsCTE WHERE rn=1;
    PRINT '  Data inserted into Logs (transformed).';
    -- Reports va ScheduledPayments uchun ham xuddi shunday qiling...

-- === QADAM 2: Foreign Keylarni Qo'shish (FINAL jadvallarga) ===
    PRINT '[Constraints] Adding Foreign Keys to FINAL tables...';
    -- FK nomlari va ustun nomlari FINAL jadvallarga mos kelishi kerak
    ALTER TABLE [dbo].[Cards] ADD CONSTRAINT [FK_Cards_Users] FOREIGN KEY ([UserID]) REFERENCES [dbo].[Users]([UserID]);
    ALTER TABLE [dbo].[Transactions] ADD CONSTRAINT [FK_Transactions_Cards_From] FOREIGN KEY ([FromCardID]) REFERENCES [dbo].[Cards]([CardID]);
    ALTER TABLE [dbo].[Transactions] ADD CONSTRAINT [FK_Transactions_Cards_To] FOREIGN KEY ([ToCardID]) REFERENCES [dbo].[Cards]([CardID]);
    ALTER TABLE [dbo].[Logs] ADD CONSTRAINT [FK_Logs_Transactions] FOREIGN KEY ([RelatedTransactionID]) REFERENCES [dbo].[Transactions]([TransactionID]);
    ALTER TABLE [dbo].[ScheduledPayments] ADD CONSTRAINT [FK_ScheduledPayments_Cards] FOREIGN KEY ([FromCardID]) REFERENCES [dbo].[Cards]([CardID]); -- FromCardID ni tekshiring
    PRINT '[Constraints] Foreign Keys added.';


    -- === QADAM 3: Derived Viewlarni Yaratish ===
    PRINT '[Views] Creating derived analytical views...';

    -- --- View 1: Fraudulent Transactions ---
    DROP VIEW IF EXISTS dbo.vw_FraudulentTransactions;
    EXEC('CREATE VIEW dbo.vw_FraudulentTransactions AS
    SELECT
        t.TransactionID, t.FromCardID, c.CardNumber AS FromCardNumber, u.UserID AS FromUserID, u.FullName AS FromUserName,
        t.ToCardID, t.TransactionAmount, t.TransactionStatus, t.TransactionTimestamp, t.TransactionType,
        t.ExceedsTransactionThreshold, t.ExceedsCardLimit
    FROM dbo.Transactions t
    LEFT JOIN dbo.Cards c ON t.FromCardID = c.CardID -- Join FINAL tables
    LEFT JOIN dbo.Users u ON c.UserID = u.UserID     -- Join FINAL tables
    WHERE t.ExceedsCardLimit = 1 OR t.ExceedsTransactionThreshold = 1;');
    PRINT '  View vw_FraudulentTransactions created.';

    -- --- View 2: VIP Users ---
    DROP VIEW IF EXISTS dbo.vw_VipUsers;
    EXEC('CREATE VIEW dbo.vw_VipUsers AS
    WITH UserTransactionStats AS (
        SELECT c.UserID, SUM(t.TransactionAmount) AS TotalAmountLast90Days, COUNT(t.TransactionID) AS TxCountLast90Days
        FROM dbo.Transactions t JOIN dbo.Cards c ON t.FromCardID = c.CardID -- Join FINAL tables
        WHERE t.TransactionTimestamp >= DATEADD(day, -90, GETDATE()) AND t.TransactionStatus = ''completed''
        GROUP BY c.UserID
    )
    SELECT
        u.UserID, u.FullName, u.EmailAddress, u.PhoneNumber, u.RegistrationDate, u.LastActivityDate, u.IsVIP, u.TotalBalance,
        ISNULL(uts.TotalAmountLast90Days, 0) AS TotalAmountLast90Days, ISNULL(uts.TxCountLast90Days, 0) AS TransactionCountLast90Days,
        CASE WHEN u.IsVIP = 1 THEN ''Flagged VIP'' WHEN u.TotalBalance > 50000 THEN ''High Balance VIP'' WHEN ISNULL(uts.TotalAmountLast90Days, 0) > 100000 THEN ''High Volume VIP'' WHEN ISNULL(uts.TxCountLast90Days, 0) > 50 THEN ''High Frequency VIP'' ELSE ''Regular'' END AS VipCategory
    FROM dbo.Users u LEFT JOIN UserTransactionStats uts ON u.UserID = uts.UserID
    WHERE u.IsVIP = 1 OR u.TotalBalance > 50000 OR ISNULL(uts.TotalAmountLast90Days, 0) > 100000 OR ISNULL(uts.TxCountLast90Days, 0) > 50;');
    PRINT '  View vw_VipUsers created.';

    -- --- View 3: Blocked Entities ---
    DROP VIEW IF EXISTS dbo.vw_BlockedEntities;
    EXEC('CREATE VIEW dbo.vw_BlockedEntities AS
    SELECT DISTINCT u.UserID, u.FullName, u.EmailAddress, u.PhoneNumber, ''User has blocked/inactive/expired cards'' AS Reason
    FROM dbo.Users u
    WHERE EXISTS (
        SELECT 1 FROM dbo.Cards c_check WHERE c_check.UserID = u.UserID AND c_check.CardStatus IN (''blocked'', ''inactive'', ''expired'') -- Check values!
    );');
    -- Agar Users jadvalida ham status bo'lsa, OR sharti qo'shish mumkin
    PRINT '  View vw_BlockedEntities created.';

-- --- View 4: Daily Summary ---
    DROP VIEW IF EXISTS dbo.vw_DailyTransactionSummary;
    EXEC('CREATE VIEW dbo.vw_DailyTransactionSummary AS
    SELECT
        CAST(t.TransactionTimestamp AS DATE) AS TransactionDate, COUNT(t.TransactionID) AS TotalTransactions, SUM(t.TransactionAmount) AS TotalAmount,
        COUNT(CASE WHEN t.TransactionStatus = ''completed'' THEN 1 END) AS SuccessfulTransactions, SUM(CASE WHEN t.TransactionStatus = ''completed'' THEN t.TransactionAmount ELSE 0 END) AS SuccessfulAmount,
        COUNT(CASE WHEN t.TransactionStatus = ''failed'' THEN 1 END) AS FailedTransactions, SUM(CASE WHEN t.TransactionStatus = ''failed'' THEN t.TransactionAmount ELSE 0 END) AS FailedAmount,
        COUNT(CASE WHEN t.TransactionStatus NOT IN (''completed'', ''failed'') THEN 1 END) AS OtherStatusTransactions
    FROM dbo.Transactions t
    GROUP BY CAST(t.TransactionTimestamp AS DATE);');
    PRINT '  View vw_DailyTransactionSummary created.';

    PRINT '[Views] All views created.';

    -- Agar shu yergacha xatosiz kelsa, commit qilamiz
    COMMIT TRANSACTION;
    PRINT '[Transaction] All SQL transformations committed successfully.';

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    PRINT '[Error] An error occurred during SQL Transformation!';
    -- Xato haqida ma'lumot (avvalgidek)
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
    PRINT 'Error Message: ' + ERROR_MESSAGE();
    -- ... boshqa xato detallari ...
END CATCH;

PRINT '--- SQL Transformation Script --- END ---';
GO