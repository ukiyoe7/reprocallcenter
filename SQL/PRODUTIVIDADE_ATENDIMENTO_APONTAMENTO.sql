WITH AGT AS (SELECT FUNNOME,USUCODIGO FROM FUNCIO F
                             LEFT JOIN USUARIO U ON U.FUNCODIGO=F.FUNCODIGO             
                            WHERE 
                            (
                            -- FILIAIS
                            F.FUNCODIGO IN (2011,2029,1613,1973)
                            OR
                            -- FUNCIONARIOS MATRIZ
                            DPTCODIGO=2
                            OR
                            -- OUTROS
                            F.FUNCODIGO IN (43,2022)
                            )),
                            
PED AS (                            
SELECT ID_PEDIDO
  FROM PEDID 
   WHERE PEDDTEMIS BETWEEN '01.05.2023' AND 'TODAY'),
   
ACPD AS (SELECT DISTINCT A.ID_PEDIDO,LPCODIGO,A.USUCODIGO
                 FROM ACOPED A
                  INNER JOIN PED P ON A.ID_PEDIDO=P.ID_PEDIDO
                   INNER JOIN AGT  ON AGT.USUCODIGO=A.USUCODIGO
                    WHERE LPCODIGO IN (1,1862,2026,2035)) 
                  
SELECT 
    FUNNOME NOME,
    SUM(CASE WHEN LPCODIGO = 1 THEN 1 ELSE 0 END) AS CAPTACAO,
    SUM(CASE WHEN LPCODIGO = 1862 THEN 1 ELSE 0 END) AS RETORNO_RESOLVIDO,
    SUM(CASE WHEN LPCODIGO = 2026 THEN 1 ELSE 0 END) AS MOTIVO_GARANTIA,
    SUM(CASE WHEN LPCODIGO = 2035 THEN 1 ELSE 0 END) AS ANALISE_GARANTIA_FILIAL
FROM ACPD A
LEFT JOIN AGT ON AGT.USUCODIGO=A.USUCODIGO
GROUP BY 1;
   
   
   
   