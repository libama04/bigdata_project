-- Independent recomputation of the yearly late-arrival rate, straight from the RAW csv.
-- Same business rules as the Spark job, written separately in SQL:
--   a row counts only if trains were planned and cancellations do not exceed them.
SELECT
    CAST(substr("date", 1, 4) AS INTEGER)                              AS year,
    service,
    SUM(nb_train_prevu - nb_annulation)                                AS trains_circulated,
    SUM(nb_train_retard_arrivee)                                       AS late_arrivals,
    SUM(nb_train_retard_arrivee) / SUM(nb_train_prevu - nb_annulation) AS late_arrival_rate
FROM raw
WHERE nb_train_prevu > 0
  AND nb_annulation <= nb_train_prevu
GROUP BY 1, 2
ORDER BY 1, 2;
