SELECT count(1) FROM control_executors ;  --0
SELECT count(1) FROM control_singleton ;  --0
SELECT count(1) FROM control_area ;       --0
SELECT count(1) FROM control_process ;    --20
SELECT count(1) FROM control_processexec ;			--61
SELECT count(1) FROM control_processexecparams ;	--195
SELECT count(1) FROM control_processexecstep ;		--596
SELECT count(1) FROM control_rawfiles ;				--1
SELECT count(1) FROM control_rawfilesdet ;          --1
SELECT count(1) FROM control_rawfilesdetfields ;	--11
SELECT count(1) FROM control_rawfilesuse ;			--20
SELECT count(1) FROM control_tables ;				--7
SELECT count(1) FROM control_tablesrel ;			--1
SELECT count(1) FROM control_tablesrelcol ;         --1
--SELECT table_id, count(1) FROM control_columns GROUP BY table_id;
SELECT count(1) FROM control_columns;				--228
SELECT count(1) FROM control_tablesuse ;			--23
SELECT count(1) FROM control_dq ;					--119
SELECT count(1) FROM control_error ;                --13 
SELECT count(1) FROM control_date ;                 --0
SELECT count(1) FROM control_testplan ;				--712
SELECT count(1) FROM control_testplanfeature ;		--1203

SELECT testplan_isok, count(1)			--17 de 1
FROM control_testplan
WHERE testplan_name = 'TestPlan_IsOK'
group by testplan_isok
