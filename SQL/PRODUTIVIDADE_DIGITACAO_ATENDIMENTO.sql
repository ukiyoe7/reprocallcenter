

WITH PED_DT AS (SELECT ID_PEDIDO FROM PEDID WHERE PEDDTEMIS BETWEEN DATEADD(-60 DAY TO CURRENT_DATE) AND 'TODAY')

PED AS (SELECT ID_PEDIDO,CLICODIGO,PEDDTEMIS,FUNCODIGO2,PEDORIGEM,FISCODIGO2 
           FROM PEDID P
            INNER JOIN PED_DT D ON P.ID_PEDIDO=D.ID_PEDIDO
             WHERE PEDORIGEM IN ('D','E')),

PED_GR (SELECT ID_PEDIDO,CLICODIGO,PEDDTEMIS,PEDORIGEM,FISCODIGO2 
                FROM PEDID 
                  INNER JOIN PED_DT D ON P.ID_PEDIDO=D.ID_PEDIDO
                   WHERE FISCODIGO1 IN ('5.94L','5.94G','5.91O'))

USER AS (SELECT FUNNOME,FUNCODIGO,USUNOME FROM FUNCIO F
                          LEFT JOIN USUARIO  U ON U.USUCODIGO=F.FUNCODIGO                
                            WHERE 
                            (
                            -- FILIAIS
                            FUNCODIGO IN (2011,2029,1613,1973)
                            OR
                            -- FUNCIONARIOS MATRIZ
                            DPTCODIGO=2
                            OR
                            -- OUTROS
                            FUNCODIGO IN (43,2022)
                            )

SELECT EMPCODIGO EMPRESA,
           A.ID_PEDIDO,
            FISCODIGO2 CFOP, 
            PEDORIGEM,
             CLICODIGO,
              USUNOME USUARIO,
               FUNNOME NOME,
                APDATA 
                FROM ACOPED A
INNER JOIN PED P ON A.ID_PEDIDO=P.ID_PEDIDO
INNER JOIN USER U ON A.USUCODIGO=U.USUCODIGO
WHERE LPCODIGO=1 

UNION

SELECT EMPCODIGO EMPRESA,
           A.ID_PEDIDO,
            FISCODIGO2 CFOP,
            PEDORIGEM,
             CLICODIGO,
              USUNOME USUARIO,
               FUNNOME NOME,
                APDATA 
                FROM ACOPED A2
INNER JOIN PED_GR PG ON A2.ID_PEDIDO=PG.ID_PEDIDO
INNER JOIN USER U2 ON A2.USUCODIGO=U2.USUCODIGO
WHERE LPCODIGO=1



