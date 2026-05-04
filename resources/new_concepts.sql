INSERT INTO concept (retired,short_name,description,form_text,datatype_id,class_id,is_set,creator,date_created,version,changed_by,date_changed,retired_by,date_retired,retire_reason,uuid) VALUES
	 (0,NULL,NULL,NULL,4,10,1,1,'2022-09-22 15:37:45',NULL,NULL,NULL,NULL,NULL,NULL,'5f21ab43-ec32-44b2-88e5-bc4ed2b93fba'),
	 (0,NULL,NULL,NULL,4,11,0,1,'2022-09-22 15:41:07',NULL,NULL,NULL,NULL,NULL,NULL,'418bd7fa-bb72-43ca-b07f-e25014ca3541'),
	 (0,NULL,NULL,NULL,4,11,0,1,'2022-09-22 15:43:13',NULL,NULL,NULL,NULL,NULL,NULL,'746532ff-e36a-47bc-8a6e-7d434f533fdd'),
	 (0,NULL,NULL,NULL,4,11,0,1,'2022-09-22 15:44:20',NULL,NULL,NULL,NULL,NULL,NULL,'946ee522-4132-4b50-9dd6-c1aed0fb0449'),
	 (0,NULL,NULL,NULL,4,11,0,1,'2022-09-22 15:46:42',NULL,NULL,NULL,NULL,NULL,NULL,'858d209d-55c2-418c-82e0-fb4f4057c5db'),
	 (0,NULL,NULL,NULL,4,11,0,1,'2022-09-22 15:47:30',NULL,NULL,NULL,NULL,NULL,NULL,'45a1b710-00cf-4697-af09-5b2269478a20'),
	 (0,NULL,NULL,NULL,4,9,0,1,'2022-09-22 15:48:51',NULL,NULL,NULL,NULL,NULL,NULL,'54d88677-b3cb-4f42-88be-5ce657a8c23e'),
	 (0,NULL,NULL,NULL,4,11,0,1,'2022-09-22 15:51:03',NULL,NULL,NULL,NULL,NULL,NULL,'1c041723-8632-47f3-81de-769274dc4c00'),
	 (0,NULL,NULL,NULL,4,11,0,1,'2022-09-22 15:51:29',NULL,NULL,NULL,NULL,NULL,NULL,'8181afee-dfc1-4488-9e02-c8a1c3d819ee'),
	 (0,NULL,NULL,NULL,4,11,0,1,'2022-09-22 15:52:41',NULL,NULL,NULL,NULL,NULL,NULL,'51a1bb37-cd63-43e9-89f6-2833044f383d');

INSERT INTO concept_name (concept_id,name,locale,locale_preferred,creator,date_created,concept_name_type,voided,voided_by,date_voided,void_reason,uuid) VALUES
	 (789,'Dosing unit','en',1,1,'2022-09-22 15:37:45','FULLY_SPECIFIED',0,NULL,NULL,NULL,'3061006d-426d-4c07-a93a-55517ed214ab'),
	 (789,'Dosing unit','en',0,1,'2022-09-22 15:37:45','SHORT',0,NULL,NULL,NULL,'12a109d3-adcd-47e9-bd7e-18f95515f478'),
	 (790,'Ampules','en',1,1,'2022-09-22 15:41:07',NULL,0,NULL,NULL,NULL,'133f1d2b-8063-4234-aec3-eb1a1b628185'),
	 (790,'Ampule','en',0,1,'2022-09-22 15:41:07','SHORT',0,NULL,NULL,NULL,'57b3bb7c-56d6-4806-a84c-294d9ff34f38'),
	 (790,'Ampule(s)','en',0,1,'2022-09-22 15:41:07','FULLY_SPECIFIED',0,NULL,NULL,NULL,'5b6b15c1-aee3-447d-b89b-583db582d58e'),
	 (791,'Gram(s)','en',1,1,'2022-09-22 15:43:13','FULLY_SPECIFIED',0,NULL,NULL,NULL,'72a44415-5802-4111-9a69-4d3788692a39'),
	 (791,'Grams','en',0,1,'2022-09-22 15:43:13','SHORT',0,NULL,NULL,NULL,'a819f937-75bf-4479-ab5f-68cf5df71138'),
	 (792,'Kilograms','en',0,1,'2022-09-22 15:44:20',NULL,0,NULL,NULL,NULL,'21bece69-7729-49b4-8ba9-c8ca70353072'),
	 (792,'Kilogram(s)','en',1,1,'2022-09-22 15:44:20','FULLY_SPECIFIED',0,NULL,NULL,NULL,'8978241d-0b6f-4ee8-8950-f65797809cb6'),
	 (792,'Kilo','en',0,1,'2022-09-22 15:44:20','SHORT',0,NULL,NULL,NULL,'20a0c522-7bd5-475b-82f2-72eca752e971');
INSERT INTO concept_name (concept_id,name,locale,locale_preferred,creator,date_created,concept_name_type,voided,voided_by,date_voided,void_reason,uuid) VALUES
	 (789,'Measurement unit','en',0,1,'2022-09-22 15:46:03',NULL,0,NULL,NULL,NULL,'d7a0e9df-3343-4539-ba96-ce99948075c3'),
	 (793,'Milligrams','en',0,1,'2022-09-22 15:46:42',NULL,0,NULL,NULL,NULL,'1f9a0440-bc49-4ed6-8e33-a4c90a67f24a'),
	 (793,'Milligram(s)','en',1,1,'2022-09-22 15:46:42','FULLY_SPECIFIED',0,NULL,NULL,NULL,'a3dede6c-24b8-4881-a45d-7f3299e92a13'),
	 (793,'MG','en',0,1,'2022-09-22 15:46:42','SHORT',0,NULL,NULL,NULL,'51243f53-45e3-4719-a6b2-c7a12fab7689'),
	 (794,'Litre(s)','en',1,1,'2022-09-22 15:47:30','FULLY_SPECIFIED',0,NULL,NULL,NULL,'ff1b53f1-00ed-4720-a2d1-f17a737e59b8'),
	 (794,'LT','en',0,1,'2022-09-22 15:47:30','SHORT',0,NULL,NULL,NULL,'2b62ebe9-146c-41a8-babd-e7b86bea751a'),
	 (794,'Litres','en',0,1,'2022-09-22 15:47:30',NULL,0,NULL,NULL,NULL,'20dcea91-4465-42d7-8d44-28ef22bec952'),
	 (795,'Millilitres','en',0,1,'2022-09-22 15:48:51',NULL,0,NULL,NULL,NULL,'9bf145a2-69eb-4f11-8fec-b3b494ff40d3'),
	 (795,'Millilitre(s)','en',1,1,'2022-09-22 15:48:51','FULLY_SPECIFIED',0,NULL,NULL,NULL,'9ecebf54-af9a-41d0-a121-9d8165172922'),
	 (795,'ML','en',0,1,'2022-09-22 15:48:51','SHORT',0,NULL,NULL,NULL,'3a2119af-2383-45da-8820-9a1c955784a1');
INSERT INTO concept_name (concept_id,name,locale,locale_preferred,creator,date_created,concept_name_type,voided,voided_by,date_voided,void_reason,uuid) VALUES
	 (796,'Teaspoon(s)','en',1,1,'2022-09-22 15:51:03','FULLY_SPECIFIED',0,NULL,NULL,NULL,'6cbad4fa-6d5c-456f-b983-9eb556949998'),
	 (796,'Teaspoons','en',0,1,'2022-09-22 15:51:03','SHORT',0,NULL,NULL,NULL,'1a48e655-0149-4d1f-b496-344c852289fd'),
	 (797,'Tablespoons','en',0,1,'2022-09-22 15:51:29','SHORT',0,NULL,NULL,NULL,'734fa66f-329a-4afb-912c-c1cce251762d'),
	 (797,'Tablespoon(s)','en',1,1,'2022-09-22 15:51:29','FULLY_SPECIFIED',0,NULL,NULL,NULL,'ce216953-f904-4fdd-9888-dc2cefbec681'),
	 (798,'Syringes','en',0,1,'2022-09-22 15:52:41','SHORT',0,NULL,NULL,NULL,'667b7a8e-46ed-486d-90bc-acbe4cf43677'),
	 (798,'Syringe(s)','en',1,1,'2022-09-22 15:52:41','FULLY_SPECIFIED',0,NULL,NULL,NULL,'bd41d5be-e289-41ef-8e4c-124b8630c7e2');

INSERT INTO concept_set (concept_id,concept_set,sort_weight,creator,date_created,uuid) VALUES
	 (790,789,0.0,1,'2022-09-22 15:54:07','36f6934a-77b5-420e-8d69-8af803c7ef02'),
	 (791,789,1.0,1,'2022-09-22 15:54:07','2a7482af-97a5-4141-8022-526231dbd5cd'),
	 (792,789,2.0,1,'2022-09-22 15:54:07','01876b75-7964-4aae-84cd-9a6f12302e29'),
	 (794,789,3.0,1,'2022-09-22 15:54:07','ea31d309-8912-4cbe-b458-8b17c36d7b28'),
	 (793,789,4.0,1,'2022-09-22 15:54:07','c1f3e8bf-51bb-41ba-80f0-d1aca40d8a04'),
	 (795,789,5.0,1,'2022-09-22 15:54:07','bde89152-c366-4e23-be2c-dcac6a682714'),
	 (796,789,6.0,1,'2022-09-22 15:54:07','4f8225d4-c764-40b2-9a86-db748d4325aa'),
	 (797,789,7.0,1,'2022-09-22 15:54:07','e4756744-c5e5-430f-a170-7bdde2c90fe7'),
	 (798,789,8.0,1,'2022-09-22 15:54:07','8bfc67b7-a135-4728-9bbd-6016659da416'),
	 (303,789,9.0,1,'2022-09-22 15:54:07','f8946735-935b-4cfb-a4fb-815fef9219f1');

INSERT INTO concept_description (concept_id,description,locale,creator,date_created,changed_by,date_changed,uuid) VALUES
	 (789,'Unit of measuring quantity of something','en',1,'2022-09-22 15:37:45',1,'2022-09-22 15:46:03','cbddaec4-1661-4963-8e81-904beaeecc3c'),
	 (790,'A unit of measurement','en',1,'2022-09-22 15:41:07',NULL,NULL,'af79d2c3-c858-4eda-8148-04395c0ef43e'),
	 (791,'A unit of measurement','en',1,'2022-09-22 15:43:13',NULL,NULL,'72597ecf-ad95-4942-88cf-553252537b73'),
	 (792,'A unit of measurement','en',1,'2022-09-22 15:44:20',NULL,NULL,'93b51df9-fc31-44f9-94e5-239aeb6b2d2c'),
	 (793,'A unit of measurement','en',1,'2022-09-22 15:46:42',NULL,NULL,'74741363-fc27-4b0b-b6c4-daef89fad142'),
	 (794,'A unit of measurement','en',1,'2022-09-22 15:47:30',NULL,NULL,'ead1597e-589f-41fb-8dfc-4e7324763fe5'),
	 (795,'A unit of measurement','en',1,'2022-09-22 15:48:51',NULL,NULL,'90e65e30-5cba-49d7-ba19-0f8ab481fb03'),
	 (796,'A unit of measurement','en',1,'2022-09-22 15:51:03',NULL,NULL,'659167d9-8a5f-473e-8b55-c518a787c803'),
	 (797,'A unit of measurement','en',1,'2022-09-22 15:51:29',NULL,NULL,'244392f9-c9db-46d7-b7fe-a5c84e86cb77'),
	 (798,'A unit of measurement','en',1,'2022-09-22 15:52:41',NULL,NULL,'5ebcd889-9db7-4353-aee5-0203d2c177c6');
