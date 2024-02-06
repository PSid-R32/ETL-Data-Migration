### Extracting the Data
The client provided a .zip file containing historical data that needed to be migrated into our system.

### Using SQL to Transform Data
Beginning any data analytics project revolves around look at the data you are provided with. Almost always, it needs to be worked and cleaned up.

### Loading the Data
Once the script was tested and successfully ran, the data conversion was validated to ensure all necessary information was imported as expected.

## The Code


	sp_addlinkedserver *SERVER_NAME* ,'SQL Server'
	SELECT * FROM OPENQUERY(*SERVER_NAME* , 'SELECT * FROM [*HISTORICAL_IMPORT*]..[*Client_Profile*]')

	--PREPPING AND CLEANING DATA
 
	SELECT * INTO billing_episodes_t FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[BillIng_Episodes]')
	GO
	
	SELECT * INTO Billing_Payers FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[BillIng_Payers]')
	GO
	
	SELECT * INTO case_notes FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[case_notes]')
	GO
	
	SELECT * INTO case_notes_types FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[case_notes_types]')
	GO
	
	SELECT * INTO case_note_details FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[case_note_details]')
	GO
	
	SELECT * INTO [Client_Profile] FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[Client_Profile]')
	GO
	
	SELECT * INTO [Client_Address] FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[Client_Address]')
	GO
	
	SELECT * INTO [Patient_Address] FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[Patient_Address]')
	GO
	
	SELECT * INTO [Patient_Images] FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[Patient_Images]')
	GO
	
	SELECT * INTO Codes_Journal FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[Codes_Journal]')
	GO
	
	SELECT * INTO hair_colors FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[hair_colors]')
	GO
	
	SELECT * INTO eye_colors FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[eye_colors]')
	GO
	
	SELECT * INTO race_types FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[race_types]')
	GO
	
	SELECT * INTO marital_status FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[marital_status]')
	GO
	
	SELECT * INTO employment_statuses FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[employment_statuses]')
	GO

	SELECT * INTO dose_information FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[dose_information]')
	GO
	
	SELECT * INTO dose_types FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[dose_types]')
	GO
	
	SELECT * INTO drug_types FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[drug_types]')
	GO
	
	SELECT * INTO dose_schedule FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[dose_schedule]')
	GO
	
	SELECT * INTO schedule_types FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[schedule_types]')
	GO
	
	SELECT * INTO [Patient_Profile_Details] FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[Patient_Profile_Details]')
	GO
	
	SELECT * INTO icd_Codes FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[icd_Codes]')
	GO
	
	SELECT * INTO Profile_flags FROM OPENQUERY([*SERVER_NAME*\SQL2022] , 'SELECT * FROM *HISTORICAL_IMPORT*..[Profile_flags]')
	GO
	
	SELECT Cast(Rtrim(T.display_id) AS VARCHAR(12)) AS account
	,last_name = Cast(Rtrim(T.last_name) AS VARCHAR(20))
	,first_name = Cast(Rtrim(T.first_name) AS VARCHAR(15))
	,mi = Cast(Rtrim(T.middle_initial) AS VARCHAR(1))
	,addr1 = Cast(Rtrim(A.street_address1) AS VARCHAR(30))
	,addr2 = Cast(Rtrim(A.street_address2) AS VARCHAR(30))
	,city = Cast(Rtrim(A.CITY) AS VARCHAR(27))
	,STATE = Cast(Rtrim(A.STATE) AS VARCHAR(2)) 
	,zip = Cast(Rtrim(Replace(A.ZIP, '-', '')) AS VARCHAR(10)) 
	,T.birthdate
	,sex = CASE  
		WHEN T.gender = 2
			THEN 'F'
		WHEN T.gender = 1
			THEN 'M'
		ELSE 'U'
		END
	,ssno = Cast(Rtrim(Replace(T.SSN, '-', '')) AS VARCHAR(11))
	,CASE 
		WHEN active = 1
			THEN 'Y'
		ELSE 'N'
		END AS Active_Profile
	,H.description AS hair_color
	,E.description AS eye_color
	,R.description AS race
	,M.description AS marital_status
	,EM.description AS employment_status
	INTO [Patients]
	FROM [Client_Profile] T
	LEFT JOIN hair_colors H ON H.id = T.hair_color_id
	LEFT JOIN eye_colors E ON E.id = T.eye_color_id
	LEFT JOIN race_types R ON R.id = T.race_id
	LEFT JOIN marital_status M ON M.id = T.marital_status_id
	LEFT JOIN employment_statuses EM ON EM.id = T.employment_status_id
	OUTER APPLY (
		SELECT *
		FROM [Client_Address] TA
		LEFT JOIN [Client_Address] A ON A.id = TA.address_id
		WHERE TA.client_id = T.id
			AND A.address_type_id = 2
		) A

	--IMPORTING PATIENT INSURANCE POLICIES
	
	SELECT CCP.display_id AS account
		,BPJ.payer_name
		,BPJ.abbreviation insurance_name
		,BPJ.id_code AS payor_id
		,subscriber_id_code AS insurance_id
		,subscriber_id_code
		,BEJ.effective_date
		,BEJ.expiration_date
		,CCJ.[description]
		,BEJ.insured_group_plan_name
		,BEJ.insured_group_plan_number
		,A.street_address1 AS addr1
		,A.address_string2 AS addr2
		,A.city
		,A.STATE
		,A.zip
		,CASE 
			WHEN cc_sequence_number_natural_id = 27
				THEN 1
			WHEN cc_sequence_number_natural_id = 28
				THEN 2
			WHEN cc_sequence_number_natural_id = 29
				THEN 3
			END AS seq
	INTO [#TEMP_TABLE]
	FROM BillIng_Episodes BEJ
	INNER JOIN Client_Profile CCP ON CCP.id = BEJ.patient_id
	INNER JOIN Billing_Payers BPJ ON BPJ.natural_id = BEJ.payer_natural_id
	INNER JOIN Codes_Journal CCJ ON CCJ.natural_id = BEJ.cc_sequence_number_natural_id
	LEFT JOIN Patient_Address A ON A.address_type_id = 106
		AND A.id = BPJ.common_address_id
	WHERE BEJ.deleted = 0
		AND BEJ.current_version = 1
		AND BPJ.current_version = 1
		AND CCJ.current_version = 1
		AND CCJ.deleted = 0
		AND cc_sequence_number_natural_id IN (
			27
			,28
			,29
			)
	GO
	
	--END RUN IN LEGACY DATABASE TO COPY TEMP TABLES INTO NEW SYSTEM
	--RUN IN NEW SYSTEM DATABASE
	
	UPDATE [TEMP_TABLE]
	SET insurance_id = Upper(insurance_id)
	
	UPDATE [TEMP_TABLE]
	SET insurance_id = Replace(insurance_id, '-', '')

	UPDATE [TEMP_TABLE]
	SET insurance_id = Replace(insurance_id, ' ', '')
	
	UPDATE [Patients]
	SET Mi = NULL
	WHERE mi = ''
	
	UPDATE [Patients]
	SET account = NULL
	WHERE account = ''
	
	DELETE
	FROM [Patients]
	WHERE account IS NULL
	
	INSERT INTO Basic_Patient (
		account
		,last_name
		,first_name
		,mi
		,addr1
		,addr2
		,city
		,STATE
		,zip
		,date_of_birth
		,sex
		,ssno
		)
	SELECT account = CAST(RTRIM(account) AS VARCHAR(12))
		,last_name = CAST(RTRIM(last_name) AS VARCHAR(20))
		,first_name = CAST(RTRIM(first_name) AS VARCHAR(15))
		,mi = CAST(RTRIM(mi) AS VARCHAR(1))
		,addr1 = CAST(RTRIM(addr1) AS VARCHAR(30))
		,addr2 = CAST(RTRIM(addr2) AS VARCHAR(30))
		,city = CAST(RTRIM(CITY) AS VARCHAR(27))
		,STATE = CAST(RTRIM(STATE) AS VARCHAR(2))
		,zip = CAST(RTRIM(REPLACE(ZIP, '-', '')) AS VARCHAR(10))
		,birthdate
		,sex = CAST(sex AS CHAR(1))
		,ssno = CAST(RTRIM(REPLACE(SSNo, '-', '')) AS VARCHAR(11))
	FROM [Patients]
	GO
	
	INSERT INTO Insurance_Holders (account)
	SELECT account
	FROM Basic_Patients
	WHERE account NOT IN (
			SELECT account
			FROM Insurance_Holders
			);
	GO
	
	INSERT INTO Patient_Account (account)
	SELECT account
	FROM Basic_Patients
	WHERE account NOT IN (
			SELECT account
			FROM Patient_Account
			);
	GO
	
	--IMPORTING PROFILE PHOTOS 
	
	INSERT INTO Profile_Photos(account, image_body)
	SELECT Cast(Rtrim(T.display_id) AS VARCHAR(12)) AS account
		,P.image_data
	
	FROM [Client_Profile] T
	INNER JOIN [Patient_Images] P ON P.key_id = T.id
	WHERE P.image_type = 1
	 AND T.display_id IN(SELECT account FROM People)
	
	
	--INSERT INTO Marital_Status_View VALUES('O','Significant Other')
	--INSERT INTO Marital_Status_View VALUES('C','Cohabitation')
	
	UPDATE [Patient_Account]
	SET marital_status = 'D'
	WHERE marital_status = 'Divorced'

	UPDATE [Patient_Account]
	SET marital_status = 'M'
	WHERE marital_status = 'Married'
	
	UPDATE [Patient_Account]
	SET marital_status = 'S'
	WHERE marital_status = 'Single'
	
	UPDATE [Patient_Account]
	SET marital_status = 'U'
	WHERE marital_status = 'Unknown'
	
	UPDATE [Patient_Account]
	SET marital_status = 'X'
	WHERE marital_status = 'Separated'
	
	UPDATE [Patient_Account]
	SET marital_status = 'O'
	WHERE marital_status = 'Significant Other'
	
	UPDATE [Patient_Account]
	SET marital_status = 'W'
	WHERE marital_status = 'Widowed'
	
	UPDATE [Patient_Account]
	SET marital_status = 'C'
	WHERE marital_status = 'Cohabitation'
	
	UPDATE Hair_Colors_View
	SET hair_color = 'Blonde'
	WHERE hair_color = 'Blond/Strawberry'
	GO
	UPDATE Hair_Colors_View
	SET hair_color = 'Black'
	WHERE hair_color = 'Brunette'
	GO
	UPDATE Hair_Colors_View
	SET hair_color = 'Red'
	WHERE hair_color = 'Red/Auburn'
	GO
	INSERT INTO Hair_Colors_View Values(15,'Brunette')
	INSERT INTO Eye_Colors_View Values(7,'Green')
	INSERT INTO Eye_Colors_View Values(8,'Hazel')
	INSERT INTO Eye_Colors_View Values(9,'Unknown')
	INSERT INTo Employment_Status_View VALUES('10','Employed')
	INSERT INTo Employment_Status_View VALUES('11','Disabled')
	INSERT INTo Employment_Status_View VALUES('12','Student (Part Time)')
	INSERT INTo Employment_Status_View VALUES('13','Student (Full Time)')
	
	UPDATE Employment_Status_View
	SET employment_status_desc = 'Unemployed'
	WHERE employment_status_desc = 'Not employed'
	*/
	GO
	
	UPDATE [Patient_Account]
	SET employment_status = '10'
	WHERE employment_status = 'Employed'
	GO
	
	UPDATE [Patient_Account]
	SET employment_status = '12'
	WHERE employment_status = 'Student (Part Time)'
	GO
	UPDATE [Patient_Account]
	SET employment_status = '13'
	WHERE employment_status = 'Student (Full Time)'
	GO
	
	
	UPDATE [Patient_Account]
	SET employment_status = '2'
	WHERE employment_status = 'Employed part-time'
	GO
	
	UPDATE [Patient_Account]
	SET employment_status = '3'
	WHERE employment_status = 'Unemployed'
	GO
	
	UPDATE [Patient_Account]
	SET employment_status = '9'
	WHERE employment_status = 'Unknown'
	GO
	
	UPDATE [Patient_Account]
	SET employment_status = '11'
	WHERE employment_status = 'Disabled'
	GO
	
	INSERT INTO Profile_Flags (
		account
		,statement_flag
		,pat_active
		,signature_source
		,person_is_patient
		,pat_marital_status
		,employment_status
		)
	SELECT account
		,'Y'
		,pat_active
		,'B'
		,'Y'
		,marital_status
		,employment_status
	FROM [Patient_Account]
	WHERE account NOT IN (
			SELECT account
			FROM Profile_Flags
			);
	GO
	
	INSERT INTO Demographics (
		account
		,hair_color
		,eye_color
		)
	SELECT account
		,hair_color
		,eye_color
	FROM [Patient_Account]
	WHERE account NOT IN (
			SELECT account
			FROM Demographics
			);
	GO
	
	INSERT INTO Master_User_ID
	SELECT account
		,'sa'
		,GETDATE()
		,GETDATE()
		,'sa'
	FROM Patients
	WHERE account NOT IN (
			SELECT account
			FROM Master_User_ID
			);
	GO
	
	INSERT INTO Action_Dates (
		account
		,release_of_info_ind
		)
	SELECT account
		,'Y'
	FROM Patients
	WHERE account NOT IN (
			SELECT account
			FROM Pat_Action_Dates
			);
	GO
	
	--INSURANCES
	
	DELETE
	FROM [TEMP_TABLE]
	WHERE insurance_name = 'N/A'
	
	DELETE
	FROM [TEMP_TABLE]
	WHERE insurance_name = 'Private Insurance'
	
	DELETE
	FROM [TEMP_TABLE]
	WHERE insurance_id IS NULL
	
	UPDATE [TEMP_TABLE]
	SET insurance_name = 'SP'
	WHERE insurance_name = 'Self Pay'
	
	UPDATE [TEMP_TABLE]
	SET insurance_name = LTRIM(RTRIM(UPPER(REPLACE(insurance_name, ' ', ''))))
	select * FROM payors
	
	INSERT INTO CONTRACTED_INSURANCES(
	insurance_name, 
	company_name,
	ADDR1,
	addr2,
	CITY,
	STATE,
	ZIP, 
	receiver_type, 
	payor_id,
	claim_processor,
	entry_date,
	change_date,
	entry_user_id, 
	change_user_id, 
	icd10,
	claim_type, 
	org_on_claim, 
	bill_prov_name_on_claim,
	ref_prov_required,
	crossover_as_billed, 
	mmis_cops,
	apg,
	alpha_diag_pointers,
	mco,
	location_on_claim,
	bill_diag1_only,
	atypical,
	balance_hold)
	SELECT
	DISTINCT CAST(insurance_name AS varchar(20)),
	insurance_name,
	addr1,
	addr2,
	city,
	state,
	zip,
	'F',
	ISNULL(payor_id,'XXXXX'),
	'NEIC',
	GETDATE(),
	GETDATE(),
	'sa',
	'sa',
	1,
	'P',
	'Y',
	'Y',
	'N',
	'N',
	'N',
	'N',
	1,
	0,
	'Y',
	0,
	0,
	0
	FROM [TEMP_TABLE]
	WHERE  CAST(insurance_name AS varchar(20)) NOT IN(SELECT insurance_name FROM Payors)
	
	--DELETE POLICIES OLDER THAN 90 days
	
	DELETE
	FROM [TEMP_TABLE]
	WHERE ISNULL(expiration_Date, '6/5/2023') < DATEADD(day, - 90, '6/5/2023')
	
	--IF DUPLICATES RETURNED STOP
	
	ALTER TABLE [TEMP_TABLE] ADD unique_id INT IDENTITY (
		1
		,1
		)
	
	DELETE
	FROM [TEMP_TABLE]
	WHERE effective_date < (
			SELECT MAX(effective_date)
			FROM [TEMP_TABLE] B
			WHERE B.insurance_name = [TEMP_TABLE].insurance_name
				AND B.insurance_id = [TEMP_TABLE].insurance_id
			)
	
	SELECT *
	FROM [TEMP_TABLE]
	WHERE unique_id > (
			SELECT MIN(unique_id)
			FROM [TEMP_TABLE] B
			WHERE B.insurance_name = [TEMP_TABLE].insurance_name
				AND B.insurance_id = [TEMP_TABLE].insurance_id
			)
	SELECT *
	INTO [TEMP_TABLE2]
	FROM [TEMP_TABLE]
	
	
	
	DELETE
	FROM [TEMP_TABLE2]
	WHERE unique_id > (
			SELECT MIN(unique_id)
			FROM [TEMP_TABLE] B
			WHERE B.insurance_name = [TEMP_TABLE2].insurance_name
				AND B.insurance_id = [TEMP_TABLE2].insurance_id
			)
	
	SELECT * FROM [TEMP_TABLE] WHERE insurance_id ='W270872859'
	
	--IF DUPLICATES ABOVE STOP!!
	
	INSERT INTO INSURANCE_DETAILS (
	account
	,insurance_id
	,insurance_name
	,ins_type_code
	,change_date
	,entry_date
	,entry_user_id
	,change_user_id
	,use_plan
	,carve_out
	,policy_effective_date
	,policy_thru_date
	)
	SELECT account = account
		,insurance_id = REPLACE(insurance_id, '-', '')
		, insurance_name = CAST(insurance_name AS varchar(20))
		,ins_type_code = 'PERSONAL PLAN'
		,change_date = Getdate()
		,entry_date = Getdate()
		,entry_user_id = 'PETER'
		,change_user_id = 'PETER'
		,use_plan = 'Y'
		,carved_out = 'N'
		,effective_date
		,expiration_date
	FROM [TEMP_TABLE2]
	WHERE insurance_name IS NOT NULL
	
	
	SELECT * FROM [TEMP_TABLE]
	
	--Insurance Profile
	
	INSERT INTO PATIENT_POLICIES (
		pat_insurance_sequence
		,account
		,pat_relation_to_insured
		,insurance_name
		,insurance_id
		)
	
	SELECT ROW_NUMBER() OVER (
			PARTITION BY ACCOUNT ORDER BY effective_date DESC
			) AS pat_insurance_sequence
		,account = Cast(Rtrim(account) AS VARCHAR(12))
		,pat_relation_to_insured = '1'
		, CAST(insurance_name AS varchar(20))
		,insurance_id = insurance_id
	FROM [TEMP_TABLE]
	WHERE account IN (
			SELECT account
			FROM Patients
			)
		AND EXISTS (
			SELECT *
			FROM INSURANCE_DETAILS
			WHERE insurance_name =  CAST([TEMP_TABLE].insurance_name AS varchar(20))
				AND insurance_id = [TEMP_TABLE].insurance_id
			)
	ORDER BY ACCOUNT
	     ,description
		,effective_date DESC
	
	--ADMIT TO CLINIC
	
	SELECT
	PE.account,
	T.*
	INTO [TEMP_PROGRAMS]
	FROM  dose_information_t T
	INNER JOIN Client_Profile CCP ON CCP.id = T.patient_id
	LEFT JOIN People PE
	 ON PE.account = CCP.display_id
	 GO
	 INSERT INTO ADMIT_PROGRAM
	            (account,
	             program_code,
	             first_contact_date,
	             program_status,
	             episode_care_id,
				 is_deleted,
				 date_of_admission,
				 program_active)
	SELECT account,
	       'OTP',
	       intake_date,
	       'A',
	       patient_id,
		   0,
		   intake_date,
		   'Y'
	FROM   [TEMP_PROGRAMS]
	GO
	
	
	--DIAGNOSIS CODES
	
	SELECT T.account,
	     T.patient_id as episode_care_id
		,DX.code_formatted
		,DX.description
		,DX.version
		,DX.code
	INTO [TEMP_DIAGS]
	FROM [TEMP_PROGRAMS] T
	INNER JOIN [Patient_Profile_Details] PP ON PP.patient_id = T.patient_id
	INNER JOIN icd_Codes DX ON DX.id = PP.primary_diagnosis_code_id
	GO
	
	DELETE FROM [TEMP_DIAGS]
	WHERE version = '9.00'
	GO
	UPDATE PP
	SET admit_diag_code = DC.diag_code
	FROM ADMIT_PROGRAM PP
	LEFT JOIN [TEMP_DIAGS] PD
	 ON PP.account = PD.account
	 AND PP.episode_care_id = PD.episode_care_id
	LEFT JOIN DIAGNOSIS_CODES DC
	ON DC.diag_code = PD.code_formatted
	WHERE DC.diag_code IS NOT NULL
	
	--INSERT MEDICATION ORDERS
	
	SELECT 
	P.account,
	P.patient_id,
	I.current_dosage AS dose_amount,
	I.intake_date AS order_date,
	I.next_return_date AS next_return_date,
	I.blind_dose,
	I.split_dose AS split_dose,
	I.dose1 AS dose_amount,
	I.dose2 AS dose_amount_2,
	I.ordering_provider AS provider_code,
	TT.days AS schedule_days,
	TT.description AS scheduled_desc,
	D.description as med_type,
	T.description as med_desc,
	CASE WHEN DS.dd_sunday = 0 THEN 1 ELSE 0 END as thb_sun,
	CASE WHEN DS.dd_monday = 0 THEN 1 ELSE 0 END AS thb_mon,
	CASE WHEN DS.dd_tuesday = 0 THEN 1 ELSE 0 END AS thb_tues,
	CASE WHEN DS.dd_wednesday = 0 THEN 1 ELSE 0 END AS thb_wed,
	CASE WHEN DS.dd_thursday = 0 THEN 1 ELSE 0 END AS thb_thur,
	CASE WHEN DS.dd_friday = 0 THEN 1 ELSE 0 END AS thb_fri,
	CASE WHEN DS.dd_saturday = 0 THEN 1 ELSE 0 END AS thb_sat,
	CASE 
		WHEN F.severity_id = 1     THEN 0
		WHEN F.severity_id = 2     THEN 1
		WHEN F.severity_id = 3	   THEN 0
		WHEN F.severity_id = 4     THEN 1
		WHEN F.severity_id IS NULL THEN 1
	END AS hold_soft,
	F.flag_date AS hold_date,
	CASE
		WHEN F.deleted = 1 THEN 0
		WHEN F.deleted = 0 THEN 1
	END AS hold_active,
	F.flag_due_date AS hold_release_date,
	F.modified_by AS release_user_id,
	F.notes AS comments,
	F.flag_start_date AS effective_date,
	F.expiration_date AS expiry_date
	INTO [TEMP_ORDERS]
	FROM dose_information I
	LEFT JOIN [TEMP_PROGRAMS] P
	 ON P.patient_id = I.patient_id
	LEFT JOIN dose_types D
	 ON I.dose_type_id = D.id
	LEFT JOIN drug_types T
	 ON I.drug_type_id = T.id
	LEFT JOIN dose_schedule DS
	 ON DS.patient_id = I.patient_id
	LEFT JOIN schedule_types TT 
	ON DS.schedule_type_id = TT.id
	LEFT JOIN flags F
	ON I.patient_id = F.patient_id
	WHERE I.current_dosage >0
	GO


	ALTER TABLE [TEMP_ORDERS] ADD order_id INT IDENTITY (1,1)
	GO
	INSERT INTO MEDICATION_ORDERS(
	[account]
	           ,[episode_care_id]
	           ,[order_id]
	           ,[order_date]
	           ,[provider_code]
	           ,[entry_user_id]
	           ,[entry_date]
	           ,[change_user_id]
	           ,[change_date]
	           ,[order_active]
	           ,[order_void]
	           ,[num_take_home]
	           ,[split_dose]
	           ,[start_date]
	           ,[end_date]
	           ,[dose_amount]
	           ,[dose_form]
	           ,[thb_mon]
	           ,[thb_tue]
	           ,[thb_wed]
	           ,[thb_thu]
	           ,[thb_fri]
	           ,[thb_sat]
	           ,[thb_sun]
	           ,[next_review_date]
	           ,[dose_amount_2]
	           ,[all_take_home])
	SELECT
	account,
	patient_id,
	order_id,
	order_date,
	provider_code,
	'sa',
	order_date,
	'sa',
	order_date,
	1,
	0,
	thb_sun + thb_mon+thb_tues+thb_wed+thb_thur+thb_fri+thb_sat,
	split_dose,
	order_date,
	DATEADD(dd,90, CAST(GETDATE() AS DATE)),
	dose_amount,
	CASE WHEN med_desc = 'Methadone' THEN 0
	WHEN med_desc = 'Buprenorphine' THEN 7
	wHEN med_desc = 'Vivitrol' THEN 3 END,
	[thb_mon],
	[thb_tues],
	[thb_wed],
	[thb_thurs],
	[thb_fri],
	[thb_sat],
	[thb_sun],
	DATEADD(dd,90, CAST(GETDATE() AS DATE)),
	null,
	0
	FROM [TEMP_ORDERS]
	
	INSERT INTO [dbo].[MED_ORDER_DETAILS]
	           ([account]
	           ,[episode_care_id]
	           ,[order_id]
	           ,[dose_date]
	           ,[dose_sequence]
	           ,[dose_form]
	           ,[dose_amount]
	           ,[take_home]
	           ,[take_home_day]
	           ,[dose_gen]
	           ,[exception_dose]
	           ,[exception_user_id]
	           ,[exception_date]
	           ,[comments]
	           ,[dose_refused]
	           ,[orig_take_home])
	SELECT
	account,
	patient_id,
	order_id,
	calendar_date,
	1 AS [dose_sequence],
	CASE WHEN med_desc = 'Methadone' THEN 0
	WHEN med_desc = 'Buprenorphine' THEN 7
	wHEN med_desc = 'Vivitrol' THEN 3 END as[dose_form],
	dose_amount,
	--DATEPART(WEEKDAY,calendar_date),
	--DATENAME(WEEKDAY,calendar_date),
	CASE WHEN DATENAME(WEEKDAY,calendar_date) = 'Sunday' AND thb_sun = 1 THEN 1
	WHEN DATENAME(WEEKDAY,calendar_date) = 'Monday' AND thb_mon = 1 THEN 1
	WHEN DATENAME(WEEKDAY,calendar_date) = 'Tuesday' AND thb_tues = 1 THEN 1
	WHEN DATENAME(WEEKDAY,calendar_date) = 'Wednesday' AND thb_wed = 1 THEN 1
	WHEN DATENAME(WEEKDAY,calendar_date) = 'Thursday' AND thb_thur = 1 THEN 1
	WHEN DATENAME(WEEKDAY,calendar_date) = 'Friday' AND thb_fri = 1 THEN 1
	WHEN DATENAME(WEEKDAY,calendar_date) = 'Saturday' AND thb_sat = 1 THEN 1
	ELSE 0 END AS take_home,
	0 AS [take_home_day],
	1 AS [dose_gen],
	0 AS [exception_dose],
	NULL AS [exception_user_id],
	NULL AS [exception_date],
	NULL AS [comments],
	0 AS [dose_refused],
	0 as [orig_take_home]
	FROM [TEMP_ORDERS], Calendar_Dates
	WHERE calendar_date > '6/11/2023'
	AND calendar_date < DATEADD(dd,90, '6/11/2023')
	AND scheduled_desc = 'Multiple Days/Week'
	ORDER BY 1,2,3,4
	
	--MEDICATION ORDER HOLDS
	
	INSERT INTO [dbo].[MEDICATION_HOLDS]
	           ([account]
	           ,[episode_care_id]
	           ,[hold_date]
	           ,[hold_user_id]
	           ,[hold_active]
	           ,[hold_type]
	           ,[hold_release_date]
	           ,[release_user_id]
	           ,[comments]
	           ,[effective_date]
	           ,[hold_priority]
	           ,[expiry_date]
	           ,[postpone_user_id]
	           ,[postpone_len]
	           ,[postpone_date]
	           ,[hold_soft])
	SELECT
		account,
		patient_id,
		hold_date,
		'sa',
		hold_user_id,
		hold_active,
		'2',
		hold_release_date,
		release_user_id,
		comments,
		effective_date,
		'0',
		expiry_date,
		NULL,
		NULL,
		NULL,
		hold_soft
	FROM [TEMP_ORDERS]
	
	/*
	UPDATE #TEMP_DX
	SET    dx = 'F43.20'
	WHERE  dx = 'F43.2'
	*/
	
	--MAP TREATMENT PLANS INTO DOCUMENTS
	
	SELECT TOP 100 *
	FROM tx_plan T
	LEFT JOIN tx_plan_types TPT ON TPT.id = T.tx_planype_id
	LEFT JOIN tx_plan_problem P ON P.tx_plan_id = T.id
	LEFT JOIN tx_plan_goal G ON G.tx_plan_problem_id = P.id
	LEFT JOIN tx_plan_method M ON M.tx_plan_goal_id = G.id
	LEFT JOIN tx_plan_outcome O ON O.tx_plan_goal_id = G.id
	LEFT JOIN tx_plan_intervention I ON I.tx_plan_parent_id = T.id
	
	/*
	SELECT TOP 100 * FROM tx_plan
	SELECT TOP 100 * FROM tx_plan_problem
	SELECT TOP 100 * FROM tx_plan_goal
	SELECT TOP 100 * FROM tx_plan_method
	SELECT TOP 100 * FROM tx_plan_outcome
	SELECT TOP 100 * FROM tx_plan_intervention
	*/
	
	--NOTE DETAILS
	
	SELECT TOP (1000) *
	FROM [DOSE_DETAILS] DTL
	LEFT JOIN [*HISTORICAL_IMPORT*].[dbo].[inv_transaction] T ON T.id = DTL.transaction
	LEFT JOIN [transaction_types] ITT ON ITT.id = T.transaction_type_id
	
	/*
	SELECT TOP 100 * FROM tx_plan
	SELECT TOP 100 * FROM tx_plan_problem
	SELECT TOP 100 * FROM tx_plan_goal
	SELECT TOP 100 * FROM tx_plan_method
	SELECT TOP 100 * FROM tx_plan_outcome
	SELECT TOP 100 * FROM tx_plan_intervention
	*/
	
	--ACTIVE CURRENT ORDERS
	
	 SELECT * FROM [TEMP_PROGRAMS]
	
	SELECT *
	FROM dose_information I
	LEFT JOIN [TEMP_PROGRAMS] P
	 ON P.patient_id = I.patient_id
	LEFT JOIN dose_types D
	 ON I.dose_type_id = D.id
	LEFT JOIN drug_types T
	 ON I.drug_type_id = T.id
	LEFT JOIN dose_schedule DS
	 ON DS.patient_id = I.patient_id
	LEFT JOIN schedule_types TT 
	ON DS.schedule_type_id = TT.id

	GO
	
	INSERT INTO [dbo].[MEDICATION_ORDERS]
	           ([account]
	           ,[episode_care_id]
	           ,[order_id]
	           ,[order_date]
	           ,[provider_code]
	           ,[entry_user_id]
	           ,[entry_date]
	           ,[change_user_id]
	           ,[change_date]
	           ,[order_active]
	           ,[order_void]
	           ,[num_take_home]
	           ,[split_dose]
	           ,[start_date]
	           ,[end_date]
	           ,[dose_amount]
	           ,[dose_form]
	           ,[thb_mon]
	           ,[thb_tue]
	           ,[thb_wed]
	           ,[thb_thu]
	           ,[thb_fri]
	           ,[thb_sat]
	           ,[thb_sun]
	           ,[next_review_date]
	           ,[dose_amount_2]
	           ,[all_take_home]
	           
	GO
	
	--CASE NOTE HISTORY
	
	INSERT INTO NOTE_LOG(encounter_id,encounter_date, account, office_location, provider_code,master_template_id, created_date, modified_user_id, modified_date,episode_care_id,is_deleted, program_code, created_user_id, active_user)
	
	--CASE NOTE HEADER
	
	SELECT
	C.id,
	C.casenotes_date,
	PP.account,
	'*CLINIC_NAME*',
	'PETER',
	211,
	C.entry_date,
	'sa',
	C.modified_date,
	PP.episode_care_id,
	0,
	PP.program_code,
	'sa',
	'sa'
	FROM  case_notes_t C
	INNER JOIN Client_Profile CCP ON CCP.id = c.patient_id
	LEFT JOIN case_notes_types CT
	 ON CT.id = C.casenotes_type
	 OUTER APPLY(SELECT TOP 1 * FROM ADMIT_PROGRAM WHERE account = CCP.display_id) PP
	 WHERE Entry_date >= '1/1/2021'
	 AND PP.account IS NOT NULL
	 GO
	 
	 
	
	
	INSERT INTO NOTE_LOG_SIGNATURES(encounter_id, route_sequence, user_id,routed_datetime, role_id, user_type, signed_date)
	SELECT
	C.id,
	1,
	'sa',
	MAX(C.entry_date),
	1,
	0,
	MAX(C.entry_date)
	FROM  case_notes_t C
	 WHERE C.id in(SELECT encounter_id FROM NOTE_LOG)		
	 AND C.id NOT IN(SELECT encounter_id FROM NOTE_LOG_SIGNATURES WHERE route_sequence = 1)
	GROUP BY C.id
	
	INSERT INTO NOTE_LOG_HISTORICAL_NOTE(encounter_id, case_note_date, note_type, service_provided_By, start_time, end_time)
	SELECT
	C.id as enc_id,
	C.casenotes_date,
	CT.description,
	C.service_provided_by,
	C.start_time,
	C.end_time
	FROM  case_notes_t C
	INNER JOIN Client_Profile CCP ON CCP.id = c.patient_id
	LEFT JOIN case_notes_types_t CT
	 ON CT.id = C.casenotes_type
	 OUTER APPLY(SELECT TOP 1 * FROM ADMIT_PROGRAM WHERE account = CCP.display_id) PP
	 WHERE Entry_date >= '1/1/2021'
	 AND C.id in(SELECT encounter_id FROM NOTE_LOG)
	 GO
	 UPDATE N
	 SET note_text = NT.note_text
	 FROM NOTE_LOG_HISTORICAL_NOTE N
	 OUTER APPLY( SELECT 
	   SS.case_note_id, 
	   (SELECT ' ' + note_text
	    FROM case_note_dtl_t US
	    WHERE US.case_note_id = SS.case_note_id
	    FOR XML PATH('')) as note_text
	FROM case_note_dtl_t SS
	WHERE ss.case_note_id = N.encounter_id
	GROUP BY SS.case_note_id
	) NT
	GO
	
	--REMOVE ASCII CHARACTERS FROM CONVERSION
	
	UPDATE NOTE_LOG_HISTORICAL_NOTE
	set note_text = REPLACE(note_text,'&#x0D;','')
	GO
	INSERT INTO Program_Forms 
	SELECT
	program_code,
	'Legacy Case Note'
	FROM Program_Codes
	GO
	SELECT MAX(encounter_id) FROM NOTE_LOG
	
	UPDATE Encounter_Numbers
	SET encounter = 100000

## CONCLUSION
Self reflecting on my work, for this import I would focus a lot more of the data cleanup within Excel to help mitigrate the number of update scripts that needed to be ran. 

Overall, I was very pleased with the data migration, and more specifically on translating discrete data into a defined historical note. that can be printed and presented to auditors. This feature that I've scripted helped the client eliminate the need to pay archival access fees to their previous vendor.  
