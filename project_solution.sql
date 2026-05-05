--Создание схемы staging
CREATE SCHEMA IF NOT EXISTS VT260427EBD88C__STAGING;

-- Удаление таблицы, если она существует
DROP TABLE IF EXISTS VT260427EBD88C__STAGING.group_log; 

-- Создание таблицы group_log для хранения логов действий пользователей в группах
CREATE TABLE VT260427EBD88C__STAGING.group_log (        
    group_id INT,
    user_id INT, 
    user_id_from INT, -- ID пользователя, который добавил (NULL, если вступил сам)
    event VARCHAR(10), -- Тип события: 'add', 'leave', 'create'
    datetime TIMESTAMP
)
ORDER BY group_id
SEGMENTED BY hash(group_id) ALL NODES
PARTITION BY datetime::DATE
GROUP BY calendar_hierarchy_day(datetime::DATE, 3, 2);

-- Загрузка данных из CSV-файла в таблицу
/*
 * Загрузка данных предполагает, что файл group_log.csv 
 * предварительно помещён в папку /data контейнера (например, через DAG в Airflow).
 */
COPY VT260427EBD88C__STAGING.group_log (
    group_id,
    user_id,
    user_id_from,
    event,
    datetime
)
FROM '/data/group_log.csv'
DELIMITER ','
ENCLOSED BY '"'
NULL AS ''
SKIP 1
;

-- Проверка количества строк
SELECT COUNT(*) FROM VT260427EBD88C__STAGING.group_log;

SELECT table_name 
FROM v_catalog.tables 
WHERE table_schema = 'VT260427EBD88C__DWH'
ORDER BY table_name;

-- Таблица связи l_user_group_activity (линк между пользователем и группой)
DROP TABLE IF EXISTS VT260427EBD88C__DWH.l_user_group_activity CASCADE;

CREATE TABLE VT260427EBD88C__DWH.l_user_group_activity
(
    hk_l_user_group_activity BIGINT PRIMARY KEY,
    hk_user_id BIGINT NOT NULL
        CONSTRAINT fk_l_user_group_activity_user REFERENCES VT260427EBD88C__DWH.h_users (hk_user_id),
    hk_group_id BIGINT NOT NULL
        CONSTRAINT fk_l_user_group_activity_group REFERENCES VT260427EBD88C__DWH.h_groups (hk_group_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);

-- Заполнение линка: уникальные пары (пользователь, группа) из staging
INSERT INTO VT260427EBD88C__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
    HASH(hu.hk_user_id, hg.hk_group_id) AS hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    NOW() AS load_dt,
    's3' AS load_src
FROM VT260427EBD88C__STAGING.group_log gl
JOIN VT260427EBD88C__DWH.h_users hu ON gl.user_id = hu.user_id
JOIN VT260427EBD88C__DWH.h_groups hg ON gl.group_id = hg.group_id
WHERE HASH(hu.hk_user_id, hg.hk_group_id) NOT IN (SELECT hk_l_user_group_activity FROM VT260427EBD88C__DWH.l_user_group_activity);

-- Сателлит s_auth_history (история действий пользователей в группах)
DROP TABLE IF EXISTS VT260427EBD88C__DWH.s_auth_history CASCADE;

CREATE TABLE VT260427EBD88C__DWH.s_auth_history
(
    hk_l_user_group_activity BIGINT NOT NULL,
    user_id_from INT,
    event VARCHAR(10),
    event_dt TIMESTAMP,
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);

-- Заполнение сателлита: история событий из staging
INSERT INTO VT260427EBD88C__DWH.s_auth_history (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT
    l.hk_l_user_group_activity,
    gl.user_id_from,
    gl.event,
    gl.datetime,
    NOW(),
    's3'
FROM VT260427EBD88C__STAGING.group_log gl
JOIN VT260427EBD88C__DWH.h_users hu ON gl.user_id = hu.user_id
JOIN VT260427EBD88C__DWH.h_groups hg ON gl.group_id = hg.group_id
JOIN VT260427EBD88C__DWH.l_user_group_activity l 
    ON l.hk_user_id = hu.hk_user_id AND l.hk_group_id = hg.hk_group_id;

SELECT COUNT(*) FROM VT260427EBD88C__DWH.l_user_group_activity;
SELECT COUNT(*) FROM VT260427EBD88C__DWH.s_auth_history;

-- Список 10 самых старых групп (по дате регистрации)
WITH oldest_groups AS (
    SELECT hk_group_id
    FROM VT260427EBD88C__DWH.h_groups
    ORDER BY registration_dt
    LIMIT 10
),
group_adds AS (  -- Пользователи, которые вступили в эти группы (event = 'add')
    SELECT
        l.hk_group_id,
        l.hk_user_id
    FROM VT260427EBD88C__DWH.l_user_group_activity l
    JOIN VT260427EBD88C__DWH.s_auth_history s 
        ON l.hk_l_user_group_activity = s.hk_l_user_group_activity
    WHERE s.event = 'add'
),
active_users AS ( -- Активные пользователи (написавшие хотя бы одно сообщение)
    SELECT DISTINCT
        l.hk_group_id,
        l.hk_user_id
    FROM group_adds l
    JOIN VT260427EBD88C__DWH.l_user_message lum ON l.hk_user_id = lum.hk_user_id
    JOIN VT260427EBD88C__DWH.l_groups_dialogs lgd ON lum.hk_message_id = lgd.hk_message_id
    WHERE lgd.hk_group_id = l.hk_group_id
)
-- 3.4. Финальный расчёт
SELECT
    a.hk_group_id,
    COUNT(DISTINCT a.hk_user_id) AS cnt_added_users,
    COUNT(DISTINCT b.hk_user_id) AS cnt_users_in_group_with_messages,
    CASE 
        WHEN COUNT(DISTINCT a.hk_user_id) = 0 THEN 0.0
        ELSE 100.0 * COUNT(DISTINCT b.hk_user_id) / COUNT(DISTINCT a.hk_user_id)
    END AS group_conversion
FROM group_adds a
LEFT JOIN active_users b ON a.hk_group_id = b.hk_group_id AND a.hk_user_id = b.hk_user_id
WHERE a.hk_group_id IN (SELECT hk_group_id FROM oldest_groups)
GROUP BY a.hk_group_id
ORDER BY group_conversion DESC;