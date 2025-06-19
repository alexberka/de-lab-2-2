## TEIS Data Cleaning

[The Tennessee Early Intervention System (TEIS)](https://www.tn.gov/disability-and-aging/disability-aging-programs/teis.html) is a program program that offers therapy and other services to families of infants and young children with developmental delays or disabilities. 

TEIS is required to report outcomes data annually on children who exited early intervention services and received a minimum of 6 months of service. This is based on the first (entrance) assessment and last (exit) assessment. For the 2023-24 data, every exit assessment used the BDI-3, but there were three possible entrance assessments, AEPS, BDI-2, or BDI-3.

TEIS currently has a set of business rules for determining a child's progress category but would like to see how these categories change under a different set of business rules. Currently, the data is not formatted in a way that this calculation is feasible. For this lab, you'll do some of the initial data cleaning and organization to facilitate this calculation. 

Write either an Azure or GCP function to process the data. This function should output two csv file. The first file should have complete records which include at least the Child ID, the entry assessment, and the DQs for Adaptive, Social-Emotional, Communication, Motor, and Cognitive. The second file should have records that are incomplete and an indicator for the reason for incompletness.

Rules for a complete record:
  1. A non-null value in the "BDI 3 ECO_Exit_DATE" column. 
  2. Exit scores (Exit SOCIAL_SCALE, Exit KNOWLEDGE_SCALE, and Exit APPROPRIATE_ACTION_SCALE) match their BDI 3 Exit scores (contained in the BDI3 Exit SOCIAL_SCALE, BDI3 Exit KNOWLEDGE_SCALE, and BDI3 Exit APPROPRIATE_ACTION_SCALE columns).
  3. A non-null value in the ECO_Entry_Date column. 
  4. There is a record in the NSS data_BDI3 Scores 20230101 20240630_Compiled_20241107.xlsx file which matches on Child ID and on assessment date; to determine this match, comare the BDI 3 ECO_Exit_DATE to the various "Date of Testing" columns. There should be at least one match. 

To determine the entry assessment, use the following rules:  
  - If the ECO_Entry_DATE matches the BDI2 Entry Date, mark that child as "BDI2".  
  - If the ECO_Entry_DATE matches the BDI3 Entry Date, mark that child as "BDI3".  
  - Otherwise, mark the child as "AEPS".  
