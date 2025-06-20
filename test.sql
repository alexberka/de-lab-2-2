SELECT column_name
FROM information_schema.columns
WHERE table_name = 'eco';

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'bdi';


select bdi_3_eco_exit_date from bdi;

select * from bdi;

select * from eco_with_bdi;
SELECT count() from eco_with_bdi;

drop table if EXISTS bdi;
drop table if EXISTS eco;
DROP TABLE IF EXISTS eco_with_bdi;

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
INTO eco_with_bdi
FROM eco
LEFT JOIN bdi
ON eco.child_id = bdi.teids_child_id
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

ALTER TABLE eco_with_bdi
ADD COLUMN entry_exam VARCHAR;

--ADD Entry exam, incomplete reason
--Populate entry exam STRING, incomplete STRING

--Make views to pull complete vs incomplete records


select bdi_3_eco_exit_date
from eco;

select * from eco_with_bdi;