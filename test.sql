CREATE OR REPLACE PROCEDURE drop_table(IN tablename VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN
	DROP TABLE IF EXISTS tablename;
END;
$$;

CREATE OR REPLACE PROCEDURE parse_records()
LANGUAGE plpgsql
AS $$
BEGIN
	DROP TABLE IF EXISTS eco_with_bdi CASCADE;

	CREATE TABLE eco_with_bdi AS
	SELECT 
		eco.child_id,
		eco.district,
		eco.eco_entry_date,
		eco.eco_exit_date,
		eco.exit_social_scale,
		eco.exit_knowledge_scale,
		eco.exit_appropriate_action_scale,
		eco.aepsi_eco_entry_date,
		eco.bdi2_entry_date,
		eco.bdi_3_eco_entry_date,
		eco.bdi_3_eco_exit_date,
		eco.bdi3_exit_social_scale,
		eco.bdi3_exit_knowledge_scale,
		eco.bdi3_exit_appropriate_action_scale,
		bdi.adaptive_developmental_quotient,
		bdi.social_emotional_developmental_quotient,
		bdi.communication_developmental_quotient,
		bdi.motor_developmental_quotient,
		bdi.cognitive_developmental_quotient,
		bdi.bdi_3_total_developmental_quotient
	FROM eco
	LEFT JOIN bdi
	ON eco.child_id = bdi.teids_child_id
	-- AND bdi.teids_child_id <> 'nan'
	AND eco.eco_exit_date IN (
		bdi.adaptive_self_care_date_of_testing,
		bdi.adaptive_personal_responsibility_date_of_testing,
		bdi.social_emotional_adult_interaction_date_of_testing,
		bdi.social_emotional_peer_interaction_date_of_testing,
		bdi.social_emotional_self_concept_social_role_date_of_testing,
		bdi.communication_receptive_communication_date_of_testing,
		bdi.communication_expressive_communication_date_of_testing,
		bdi.motor_gross_motor_date_of_testing,
		bdi.motor_fine_motor_date_of_testing,
		bdi.motor_perceptual_motor_date_of_testing,
		bdi.cognitive_attention_and_memory_date_of_testing,
		bdi.cognitive_reasoning_academic_skills_date_of_testing,
		bdi.cognitive_perception_and_concepts_date_of_testing
	)
	ORDER BY eco.child_id;

	--Add generated columns to eco_with_bdi table
	ALTER TABLE eco_with_bdi
	ADD COLUMN entry_exam TEXT GENERATED ALWAYS AS (
		CASE
			WHEN bdi_3_eco_entry_date <> '1900-01-01' THEN 'BDI-3'
			WHEN bdi2_entry_date <> '1900-01-01' THEN 'BDI-2'
			WHEN aepsi_eco_entry_date <> '1900-01-01' THEN 'AEPS'
			ELSE 'NA'
		END
	) STORED,
	ADD COLUMN missing_entry_date BOOLEAN GENERATED ALWAYS AS (
		eco_entry_date = '1900-01-01'
	) STORED,
	ADD COLUMN missing_exit_date BOOLEAN GENERATED ALWAYS AS (
		eco_exit_date = '1900-01-01'
	) STORED,
	ADD COLUMN mismatched_exit_dates BOOLEAN GENERATED ALWAYS AS (
		eco_exit_date <> bdi_3_eco_exit_date
	) STORED,
	ADD COLUMN mismatched_exit_scores BOOLEAN GENERATED ALWAYS AS (
		exit_social_scale <> bdi3_exit_social_scale
		OR exit_knowledge_scale <> bdi3_exit_knowledge_scale
		OR exit_appropriate_action_scale <> bdi3_exit_appropriate_action_scale
	) STORED,
	ADD COLUMN missing_developmental_quotients BOOLEAN GENERATED ALWAYS AS (
		CASE
			WHEN (adaptive_developmental_quotient = 0
				OR adaptive_developmental_quotient IS NULL
				OR social_emotional_developmental_quotient = 0
				OR social_emotional_developmental_quotient IS NULL
				OR communication_developmental_quotient = 0
				OR communication_developmental_quotient IS NULL
				OR motor_developmental_quotient = 0
				OR motor_developmental_quotient IS NULL
				OR cognitive_developmental_quotient = 0
				OR cognitive_developmental_quotient IS NULL
				OR bdi_3_total_developmental_quotient = 0
				OR bdi_3_total_developmental_quotient IS NULL) THEN True
			ELSE False
		END
	) STORED;

	--Generate views
	CREATE OR REPLACE VIEW complete_records AS
	SELECT
		child_id,
		district,
		eco_entry_date,
		entry_exam,
		eco_exit_date,
		adaptive_developmental_quotient AS adaptive_dq,
		social_emotional_developmental_quotient AS social_emotional_dq,
		communication_developmental_quotient AS communication_dq,
		motor_developmental_quotient AS motor_dq,
		cognitive_developmental_quotient AS cognitive_dq,
		bdi_3_total_developmental_quotient AS bdi_3_total_dq
	FROM eco_with_bdi
	WHERE (
		missing_entry_date = False
		AND missing_exit_date = False
		AND mismatched_exit_dates = False
		AND mismatched_exit_scores = False
		AND missing_developmental_quotients = False
		AND entry_exam <> 'NA'
	);

	CREATE OR REPLACE VIEW incomplete_records AS
	SELECT
		child_id,
		district,
		eco_entry_date,
		entry_exam,
		eco_exit_date,
		adaptive_developmental_quotient AS adaptive_dq,
		social_emotional_developmental_quotient AS social_emotional_dq,
		communication_developmental_quotient AS communication_dq,
		motor_developmental_quotient AS motor_dq,
		cognitive_developmental_quotient AS cognitive_dq,
		bdi_3_total_developmental_quotient AS bdi_3_total_dq,
		missing_entry_date,
		missing_exit_date,
		mismatched_exit_dates,
		mismatched_exit_scores,
		missing_developmental_quotients
	FROM eco_with_bdi
	WHERE (
		missing_entry_date = True
		OR missing_exit_date = True
		OR mismatched_exit_dates = True
		OR mismatched_exit_scores = True
		OR missing_developmental_quotients = True
		OR entry_exam = 'NA'
	);
END;
$$;
