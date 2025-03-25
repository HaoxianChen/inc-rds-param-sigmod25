/****************** 创建增量任务表(inc_rds_task) ********************/
DROP TABLE IF EXISTS "public"."inc_rds_task";
DROP SEQUENCE IF EXISTS "public"."inc_rds_task_id_seq";

CREATE SEQUENCE "public"."inc_rds_task_id_seq"
    START WITH 1000000
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE CACHE 1;
CREATE TABLE "public"."inc_rds_task"
(
    "id"          bigint DEFAULT nextval('"public".inc_rds_task_id_seq'::regclass) NOT NULL,
    "support"     numeric                                                        NOT NULL,
    "confidence"  numeric                                                        NOT NULL,
    "is_deleted"  int                                                            NOT NULL,
    "create_time" bigint                                                         NOT NULL,
    "update_time" bigint                                                         NOT NULL,
    CONSTRAINT "inc_rds_task_pkey" PRIMARY KEY ("id")
);
ALTER SEQUENCE "public"."inc_rds_task_id_seq"
    OWNER TO "postgres";
ALTER TABLE "public"."inc_rds_task"
    OWNER TO "postgres";

/****************** 创建增量规则表(inc_minimal_rule) ********************/
DROP TABLE IF EXISTS "public"."inc_minimal_rule";
DROP SEQUENCE IF EXISTS "public"."inc_minimal_rule_id_seq";

CREATE SEQUENCE "public"."inc_minimal_rule_id_seq"
    START WITH 10000000
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE CACHE 1;
CREATE TABLE "public"."inc_minimal_rule"
(
    "id"          bigint DEFAULT nextval('"public".inc_minimal_rule_id_seq'::regclass) NOT NULL,
    "task_id"     bigint                                                             NOT NULL,
    "ree"         text,
    "ree_json"    text,
    "node_json"   text                                                               NOT NULL,
    "ree_type"    int                                                                NOT NULL,
    "support"     numeric                                                            NOT NULL,
    "confidence"  numeric                                                            NOT NULL,
    "create_time" bigint                                                             NOT NULL,
    "update_time" bigint                                                             NOT NULL,
    CONSTRAINT "inc_minimal_rule_pkey" PRIMARY KEY ("id")
);
ALTER SEQUENCE "public"."inc_minimal_rule_id_seq"
    OWNER TO "postgres";
ALTER TABLE "public"."inc_minimal_rule"
    OWNER TO "postgres";

/****************** 创建增量剪枝规则表(inc_prune_rule) ********************/
DROP TABLE IF EXISTS "public"."inc_prune_rule";
DROP SEQUENCE IF EXISTS "public"."inc_prune_rule_id_seq";

CREATE SEQUENCE "public"."inc_prune_rule_id_seq"
    START WITH 10000000
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE CACHE 1;
CREATE TABLE "public"."inc_prune_rule"
(
    "id"          bigint DEFAULT nextval('"public".inc_prune_rule_id_seq'::regclass) NOT NULL,
    "task_id"     bigint                                                           NOT NULL,
    "node_json"   text                                                             NOT NULL,
    "rule_type"   int                                                              NOT NULL,
    "support"     numeric                                                          NOT NULL,
    "confidence"  numeric                                                          NOT NULL,
    "create_time" bigint                                                           NOT NULL,
    "update_time" bigint                                                           NOT NULL,
    CONSTRAINT "inc_prune_rule_pkey" PRIMARY KEY ("id")
);
ALTER SEQUENCE "public"."inc_prune_rule_id_seq"
    OWNER TO "postgres";
ALTER TABLE "public"."inc_prune_rule"
    OWNER TO "postgres";

/****************** 创建增量抽样节点表(inc_sample_node) ********************/
DROP TABLE IF EXISTS "public"."inc_sample_node";
DROP SEQUENCE IF EXISTS "public"."inc_sample_node_id_seq";

CREATE SEQUENCE "public"."inc_sample_node_id_seq"
    START WITH 10000000
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE CACHE 1;
CREATE TABLE "public"."inc_sample_node"
(
    "id"                     bigint DEFAULT nextval('"public".inc_sample_node_id_seq'::regclass) NOT NULL,
    "task_id"                bigint                                                            NOT NULL,
    "cover_radius"           int                                                               NOT NULL,
    "current_node_json"      text                                                              NOT NULL,
    "predecessor_nodes_json" text                                                              NOT NULL,
    "neighbor_nodes_json"    text                                                              NOT NULL,
    "min_confidence"         numeric                                                           NOT NULL,
    "max_confidence"         numeric                                                           NOT NULL,
    "neighbor_confs_json"    text                                                              NOT NULL,
    "neighbor_cdf_json"      text                                                              NOT NULL,
    "create_time"            bigint                                                            NOT NULL,
    "update_time"            bigint                                                            NOT NULL,
    CONSTRAINT "inc_sample_node_pkey" PRIMARY KEY ("id")
);
ALTER SEQUENCE "public"."inc_sample_node_id_seq"
    OWNER TO "postgres";
ALTER TABLE "public"."inc_sample_node"
    OWNER TO "postgres";

/****************** 创建task extend表(inc_rds_task_extend) ********************/
DROP TABLE IF EXISTS "public"."inc_rds_task_extend";
DROP SEQUENCE IF EXISTS "public"."inc_rds_task_extend_id_seq";

CREATE SEQUENCE "public"."inc_rds_task_extend_id_seq"
    START WITH 1000000
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE CACHE 1;
CREATE TABLE "public"."inc_rds_task_extend"
(
    "id"          bigint DEFAULT nextval('"public".inc_rds_task_extend_id_seq'::regclass) NOT NULL,
    "task_id"     bigint                                                                NOT NULL,
    "extend_json" text                                                                  NOT NULL,
    "create_time" bigint                                                                NOT NULL,
    "update_time" bigint                                                                NOT NULL,
    CONSTRAINT "inc_rds_task_extend_pkey" PRIMARY KEY ("id")
);
ALTER SEQUENCE "public"."inc_rds_task_extend_id_seq"
    OWNER TO "postgres";
ALTER TABLE "public"."inc_rds_task_extend"
    OWNER TO "postgres";

/****************** 创建rule表 ********************/
DROP TABLE IF EXISTS "public"."rule";
DROP SEQUENCE IF EXISTS "public"."rule_id_seq";

CREATE SEQUENCE "public"."rule_id_seq"
    START WITH 10000000
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE CACHE 1;
CREATE TABLE "public"."rule"
(
    "id"             bigint DEFAULT nextval('"public".rule_id_seq'::regclass) NOT NULL,
    "task_id"        bigint                                                   NOT NULL,
    "conflict_count" bigint                                                   NOT NULL,
    "ree"            text                                                     NOT NULL,
    "ree_json"       text                                                     NOT NULL,
    "ree_type"       int2                                                     NOT NULL,
    "support"        numeric                                                  NOT NULL,
    "confidence"     numeric                                                  NOT NULL,
    "y_column_id"    bigint                                                   NOT NULL,
    "status"         int2                                                     NOT NULL,
    "y_column_id2"   bigint                                                   NOT NULL,
    "bind_id_list"   varchar(255)                                             NOT NULL,
    CONSTRAINT "rule_pKey" PRIMARY KEY ("id")
);
ALTER SEQUENCE "public"."rule_id_seq"
    OWNER TO "postgres";
ALTER TABLE "public"."rule"
    OWNER TO "postgres";

/****************** 创建 rule_columns 表 ********************/
DROP TABLE IF EXISTS "public"."rule_columns";

CREATE TABLE "public"."rule_columns"
(
    "rule_id"   bigint NOT NULL,
    "bind_id"   bigint NOT NULL,
    "column_id" bigint NOT NULL,
    "label_y"   int2   NOT NULL DEFAULT 0,
    "task_id"   bigint NULL
);
ALTER TABLE "public"."rule_columns"
    OWNER TO "postgres";

/****************** 创建 resource_tab 表 ********************/
CREATE TABLE IF NOT EXISTS resource_tab
(
    resource_id bigserial primary key,
    user_id bigint NOT NULL DEFAULT 0,
    cpu_available int NOT NULL DEFAULT 0,
    cpu_used int NOT NULL DEFAULT 0,
    mem_available int NOT NULL DEFAULT 0,
    mem_used int NOT NULL DEFAULT 0,
    disk_available int NOT NULL DEFAULT 0,
    disk_used int NOT NULL DEFAULT 0,
    curr_task_id bigint NOT NULL DEFAULT 0,
    status smallint NOT NULL DEFAULT 0,
    create_time bigint NOT NULL DEFAULT 0,
    modify_time bigint NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_id on resource_tab (user_id);

/****************** 创建 task_tab 表 ********************/
CREATE TABLE IF NOT EXISTS task_tab
(
    task_id bigserial primary key,
    task_type smallint NOT NULL DEFAULT 0,
    user_id bigint NOT NULL DEFAULT 0,
    deploy_type int NOT NULL DEFAULT 0,
    cpu int NOT NULL DEFAULT 0,
    mem int NOT NULL DEFAULT 0,
    disk int NOT NULL DEFAULT 0,
    task_name varchar(64) NOT NULL DEFAULT '',
    container_num int NOT NULL DEFAULT 0,
    containers varchar(4096) NOT NULL DEFAULT '',
    docker_image varchar(128) NOT NULL DEFAULT '',
    status smallint NOT NULL DEFAULT 0,
    stage smallint NOT NULL DEFAULT 0,
    retry_cnt smallint NOT NULL DEFAULT 0,
    data_size bigint NOT NULL DEFAULT 0,
    req varchar(4096) NOT NULL DEFAULT '',
    resp varchar(4096) NOT NULL DEFAULT '',
    create_time bigint NOT NULL DEFAULT 0,
    modify_time bigint NOT NULL DEFAULT 0
    );
CREATE INDEX IF NOT EXISTS idx_createtime on task_tab (create_time);

insert into resource_tab(user_id,cpu_available,mem_available,create_time,modify_time) values(123,640,1280,1690524879000,1690524879000);