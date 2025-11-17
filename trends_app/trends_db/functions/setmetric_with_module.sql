-- Enhanced setmetric function that supports metric_module field
-- This is an overloaded version of the existing setmetric(text, text)

CREATE OR REPLACE FUNCTION public.setmetric(
    pMetricName TEXT,
    pMetricValue TEXT,
    pMetricModule TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $function$
DECLARE
    _metricid INTEGER;
BEGIN
    -- Try to find existing metric
    SELECT metric_id INTO _metricid
    FROM metric
    WHERE metric_name = pMetricName;

    IF FOUND THEN
        -- Update existing metric (value and module)
        UPDATE metric
        SET metric_value = pMetricValue,
            metric_module = pMetricModule
        WHERE metric_id = _metricid;
    ELSE
        -- Insert new metric
        INSERT INTO metric (metric_name, metric_value, metric_module)
        VALUES (pMetricName, pMetricValue, pMetricModule);
    END IF;

    RETURN TRUE;
END;
$function$;

COMMENT ON FUNCTION public.setmetric(TEXT, TEXT, TEXT) IS
'Upsert a metric with name, value, and optional module categorization. Overload of setmetric(TEXT, TEXT) that includes metric_module field.';
