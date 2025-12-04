/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Чезганов Алексей
 * Дата: 3.12.25
*/

/* Часть 1. Разработка витрины данных
 * Напишите ниже запрос для создания витрины данных
*/

--CREATE TABLE ds_ecom.data_mart AS
WITH user_info AS
    (
        SELECT
            *,
            last_order_ts - first_order_ts AS lifetime
        FROM (
            SELECT
                u.user_id,
                u.region,
                FIRST_VALUE(o.order_purchase_ts) OVER (PARTITION BY u.user_id)    AS first_order_ts,
                LAST_VALUE(o.order_purchase_ts) OVER (PARTITION BY u.user_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)     AS last_order_ts
            FROM ds_ecom.users      AS u
                JOIN ds_ecom.orders AS o USING (buyer_id)
            ) AS user_info_base
    ),
    user_stats AS
    (
        SELECT
            u.user_id,
            COUNT(o.order_id)                                                              AS total_orders,
            AVG(ro.review_score) FILTER (WHERE ro.review_id IS NOT NULL)                   AS avg_order_rating,
            COUNT(review_id)                                                               AS num_orders_with_rating,
            COUNT(order_status) FILTER (WHERE o.order_status = 'Отменено')                 AS num_canceled_orders,
            ROUND(COUNT(order_status)
                  FILTER (WHERE o.order_status = 'Отменено') / COUNT(*)::numeric, 4) * 100 AS canceled_order_ratio
        FROM ds_ecom.users                  AS u
            JOIN ds_ecom.orders             AS o USING (buyer_id)
            LEFT JOIN ds_ecom.order_reviews AS ro USING (order_id)
        WHERE o.order_status IN ('Отменено', 'Доставлено')
        GROUP BY u.user_id
    ),
    purchases_info AS
    (
        SELECT
            user_id,
            SUM(order_cost) FILTER ( WHERE order_status = 'Доставлено' ) AS total_order_costs,
            AVG(order_cost) AS avg_order_cost,
            -- поле: кол-во заказов в рассрочку
            -- поле: кол-во заказов с промокодами

        FROM (
            SELECT
                u.user_id,
                order_id,
                SUM(oi.price) OVER (PARTITION BY o.order_id) AS order_cost,
                order_status,
                payment_type -- обработать
            FROM ds_ecom.users              AS u
                JOIN ds_ecom.orders         AS o USING (buyer_id)
                JOIN ds_ecom.order_items    AS oi USING (order_id)
                JOIN ds_ecom.order_payments AS op USING (order_id)) AS purchases_base
    )
    -- бинарные признаки
SELECT *
FROM user_info
    JOIN user_stats USING(user_id)

-- проверить фильтрацию


/* Часть 2. Решение ad hoc задач
 * Для каждой задачи напишите отдельный запрос.
 * После каждой задачи оставьте краткий комментарий с выводами по полученным результатам.
*/

/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/

-- Напишите ваш запрос тут

/* Напишите краткий комментарий с выводами по результатам задачи 1.
 * 
*/



/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/

-- Напишите ваш запрос тут

/* Напишите краткий комментарий с выводами по результатам задачи 2.
 * 
*/



/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/

-- Напишите ваш запрос тут

/* Напишите краткий комментарий с выводами по результатам задачи 3.
 * 
*/



/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/

-- Напишите ваш запрос тут

/* Напишите краткий комментарий с выводами по результатам задачи 4.
 * 