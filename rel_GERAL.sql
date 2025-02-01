select
    t.n_aih AIH,
	t.cnes,
	t.mes_cmpt as MES_ULTIMA_APRESENTACAO,
	t.ano_cmpt AS ANO_ULTIMA_APRESENTACAO,
	t.MES_SAIDA AS MES_ALTA,
	t.ANO_SAIDA AS ANO_ALTA,
	t.qtd_aih   AS QTD_APRESENTACOES,
	t.VALOR_TOTAL AS VALOR_ULTIMA_APRESENTACAO,
	get_descricao_erro(COALESCE((select co_erro from public.sih_aih_rejeitada_erro as e where e.aih = t.n_aih and e.mes = t.mes_cmpt and e.ano = t.ano_cmpt LIMIT 1), 'ERRO NÃO LOCALIZADO')) motivo_erro
	
from (WITH CompetenciaMaisRecente AS (
	    -- Determina a competência mais recente para cada AIH
	    SELECT
	        RJ.n_aih,
	        MAX(RJ.ano_cmpt * 100 + RJ.mes_cmpt) AS competencia
	    FROM 
	        PUBLIC.sih_aih_rejeitada AS RJ
	    WHERE 
	        RJ.dt_saida BETWEEN '2018-08-01' AND '2024-06-30'
	        AND (SELECT COUNT(RD.n_aih) 
	             FROM public.sih_aih_reduzida AS RD 
	             WHERE RD.n_aih = RJ.n_aih) = 0
	    GROUP BY 
	        RJ.n_aih
	)
	SELECT 
	    RJ.n_aih,
	    RJ.cnes,
	    RJ.mes_cmpt,
	    RJ.ano_cmpt,
	    EXTRACT(MONTH FROM RJ.dt_saida) AS MES_SAIDA,
	    EXTRACT(YEAR FROM RJ.dt_saida) AS ANO_SAIDA,
	    COUNT(RJ.n_aih) AS qtd_aih,
	    SUM(RJ.VAL_TOT) AS VALOR_TOTAL
	FROM 
	    PUBLIC.sih_aih_rejeitada AS RJ
	INNER JOIN 
	    CompetenciaMaisRecente AS CMR
	    ON RJ.n_aih = CMR.n_aih
	    AND RJ.ano_cmpt * 100 + RJ.mes_cmpt = CMR.competencia
	GROUP BY 
	    RJ.n_aih, RJ.cnes, RJ.mes_cmpt, RJ.ano_cmpt, 
	    EXTRACT(MONTH FROM RJ.dt_saida), EXTRACT(YEAR FROM RJ.dt_saida)
	ORDER BY 
	    RJ.n_aih, EXTRACT(MONTH FROM RJ.dt_saida), EXTRACT(YEAR FROM RJ.dt_saida)
    ) t
