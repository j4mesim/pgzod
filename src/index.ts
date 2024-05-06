#!/usr/bin/env node

import { camelCase, pascalCase } from "change-case";
import { mkdir, rm, writeFile } from "fs/promises";
import { join } from "path";
import { DatabasePool, sql } from "slonik";
import type { Arguments, Argv } from "yargs";
import { hideBin } from "yargs/helpers";
import yargs from "yargs/yargs";
import { z } from "zod";

import type { CreatePoolProps } from "./lib/createPool";
import { createPool } from "./lib/createPool";

interface PgRelationship {
  con: number;
  conname: string;

  nsp: number;
  nspname: string;

  pnsp: number;
  pnspname: string;

  prel: number;
  prelname: string;
  preltype: number;
  prelkind: string; // r (v?, p, f, c?)
  prelnamespace: number;

  patt: number;
  pattname: string;
  patttyp: number;
  pattlen: number;
  pattndims: number;
  pattnotnull: "t" | "f";
  patthasdef: "t" | "f";
  patt_idx: number;

  fnsp: number;
  fnspname: string;

  frel: number;
  frelname: string;
  freltype: number;
  frelkind: string;
  frelnamespace: number;

  fatt: number;
  fattname: string;
  fatttyp: number;
  fattlen: number;
  fattndims: number;
  fattnotnull: "t" | "f";
  fatthasdef: "t" | "f";
  fatt_idx: number;
}

interface PgColumn {
  nsp: number;
  nspname: string;

  rel: number;
  relname: string;
  reltype: number;
  relkind: string; // r (v?, p, f, c?)
  relnamespace: number;

  att_idx: number;
  att: number;
  attname: string;
  atttyp: number;
  attlen: number;
  attndims: number;
  attnotnull: "t" | "f";
  atthasdef: "t" | "f";
}

interface PgConstraint {
  con: number;
  conname: string;
  contype: "p" | "u" | "c" | "x";

  nsp: number;
  nspname: string;

  rel: number;
  relname: string;
  reltype: number;
  relkind: string; // r (v?, p, f, c?)
  relnamespace: number;

  att_idx: number;
  att: number | null;
  attname: string | null;
  atttyp: number | null;
  attlen: number | null;
  attndims: number | null;
  attnotnull: "t" | "f" | null;
  atthasdef: "t" | "f" | null;
}

const getPgColumns = async (schema: string, pool: DatabasePool) =>
  pool.many(
    sql<PgColumn>`
  select
    n.oid as nsp,
    n.nspname,

    r.oid as rel,
    r.relname,
    r.reltype,
    r.relkind,
    r.relnamespace,

    a.attnum as att,
    a.attname,
    a.atttypid as atttyp,
    a.attlen,
    a.attndims,
    a.attnotnull,
    a.atthasdef

  from pg_class r
  left join pg_namespace n on (r.relnamespace=n.oid)
  left join pg_attribute a on (r.oid=a.attrelid)
  where nspname=${schema}
  order by nspname, relname, att;`
  );

const getPgConstraints = async (schema: string, pool: DatabasePool) =>
  pool.many(
    sql<PgConstraint>`
    with pg_constraint_indices as (
      select  
        c.oid as con,
        c.conname,
        c.contype,

        c.conrelid,
        c.conkey,
    
        generate_series(1, array_length(conkey, 1)) as conkey_idx
    
      from pg_constraint c
      where contype!='f'
    )

    select
      c.con,
      c.conname,
      c.contype,
      
      n.oid as nsp,
      n.nspname,

      r.oid as rel,
      r.relname,
      r.reltype,
      r.relkind,
      r.relnamespace,

      c.conkey_idx as att_idx,
      a.attnum as att,
      a.attname,
      a.atttypid as atttyp,
      a.attlen,
      a.attndims,
      a.attnotnull,
      a.atthasdef

    from pg_constraint_indices c
    left join pg_class r on (r.oid=c.conrelid)
    left join pg_attribute a on (r.oid=a.attrelid and c.conkey[c.conkey_idx]=a.attnum)
    left join pg_namespace n on (r.relnamespace=n.oid)
    where n.nspname=${schema}
    order by nspname, relname, att;`
  );

const getPgRelationships = (schema: string, pool: DatabasePool) =>
  pool.many(
    sql<PgRelationship>`
    with pg_columns as (
      select
        n.oid as nsp,
        n.nspname,
    
        r.oid as rel,
        r.relname,
        r.reltype,
        r.relkind,
        r.relnamespace,
    
        a.attnum as att,
        a.attname,
        a.atttypid as atttyp,
        a.attlen,
        a.attndims,
        a.attnotnull,
        a.atthasdef
    
      from pg_class r
      left join pg_namespace n on (r.relnamespace=n.oid)
      left join pg_attribute a on (r.oid=a.attrelid)
      where nspname=${schema}
      order by nspname, relname, att
    )
    
    , pg_constraint_indices as (
      select  
        c.oid as con,
        c.conname,
    
        n.oid as nsp,
        n.nspname,
    
        generate_series(1, array_length(conkey, 1)) as conkey_idx
    
      from pg_constraint c
      left join pg_namespace n on (c.connamespace=n.oid)
      where contype='f'
        and nspname=${schema}
    )
    
    select 
      i.con,
      i.conname,
    
      i.nsp,
      i.nspname,
    
      p.nsp as pnsp,
      p.nspname as pnspname,
    
      p.rel as prel,
      p.relname as prelname,
      p.reltype as preltype,
      p.relkind as prelkind,
      p.relnamespace as prelnamespace,
    
      i.conkey_idx as patt_idx,
      p.att as patt,
      p.attname as pattname,
      p.atttyp as patttyp,
      p.attlen as pattlen,
      p.attndims as pattndims,
      p.attnotnull as pattnotnull,
      p.atthasdef as patthasdef,
    
      f.nsp as fnsp,
      f.nspname as fnspname,
    
      f.rel as frel,
      f.relname as frelname,
      f.reltype as freltype,
      f.relkind as frelkind,
      f.relnamespace as frelnamespace,
    
      i.conkey_idx as fatt_idx,
      f.att as fatt,
      f.attname as fattname,
      f.atttyp as fatttyp,
      f.attlen as fattlen,
      f.attndims as fattndims,
      f.attnotnull as fattnotnull,
      f.atthasdef as fatthasdef
    
    from pg_constraint_indices i
    left join pg_constraint c on (i.con=c.oid)
    left join pg_columns p on (c.conrelid=p.rel and c.conkey[i.conkey_idx]=p.att)
    left join pg_columns f on (c.confrelid=f.rel and c.confkey[i.conkey_idx]=f.att)
    order by prelname, patt_idx;`
  );

/**
 * PGZod strategy validator.
 */
const STRATEGIES = ["read", "write", "update"] as const;
const Strategies = z.array(z.enum(STRATEGIES));
type StrategiesT = z.infer<typeof Strategies>;
/**
 * Default command name.
 */
export const command = "pgzod";
/**
 * Default command description.
 */
export const describe = "create Zod types for a postgres schema";
/**
 * Default command options.
 */
export type Options = {
  /**
   * Clean run flag
   */
  clean: boolean;
  /**
   * Custom PosgreSQL types to Zod validators separated by an equal (=) sign.
   * E.g. --customZodTypes timestamptx=z.date() date=z.date()
   */
  customZodTypes: string[];
  /**
   * Validators ouput folder
   */
  output: string;
  /**
   * Name of the schema to convert into zod validators.
   */
  schema: string;
  /**
   * Select the strategy to be used to create the `Zod` schemas.
   */
  strategy?: string;
} & CreatePoolProps;

/**
 * Yargs default command builder function.
 * Please refer to Yargs documentation to see how it works:
 * https://www.npmjs.com/package/yargs
 */
export const builder: (args: Argv<Record<string, unknown>>) => Argv<Options> = (
  y
) =>
  y
    .options({
      clean: {
        type: "boolean",
        describe: "delete the current zod schema folder",
        default: true,
      },
      customZodTypes: {
        type: "array",
        describe:
          "Custom PosgreSQL types to Zod validators separated by an equal (=) sign. E.g. --customZodTypes timestamptx=z.date() date=z.date()",
        default: [],
      },
      output: {
        type: "string",
        describe: "zod schema output folder",
        default: "/tmp/pgzod",
      },
      pgdatabase: {
        type: "string",
        describe: "database name",
        default: "postgres",
      },
      pghost: {
        type: "string",
        describe: "database host",
        default: "127.0.0.1",
      },
      pgpassword: {
        type: "string",
        describe: "database user password",
        default: "",
      },
      pgport: {
        type: "string",
        describe: "database host port",
        default: "5432",
      },
      pguser: {
        type: "string",
        describe: "database user",
        default: "postgres",
      },
      schema: {
        type: "string",
        describe: "schema to convert into zod schema",
        default: "public",
      },
      strategy: {
        type: "string",
        array: true,
        choices: ["read", "write", "update"],
        default: "write",
      },
    })
    // Deactivate the use of environment variables for option configurations.
    .env(true);
/**
 * Yargs default command handler function.
 * Defaults are duplicated to allow importing this function from another module.
 * @param argv - Command options.
 */
export const handler = async (
  argv: Arguments<Options> | Options
): Promise<void> => {
  const {
    clean = true,
    customZodTypes = [],
    output = join(__dirname, "."),
    schema = "public",
    strategy: strategies = ["write"],
    pgdatabase,
    pghost,
    pgpassword,
    pgport,
    pguser,
  } = argv;

  try {
    const pool = createPool({
      pgdatabase,
      pghost,
      pgpassword,
      pgport,
      pguser,
    });

    console.info(`Fetching table list for schema: ${schema}`);

    const columns = await getPgColumns(schema, pool);
    console.log(JSON.stringify(columns[0]));

    const constraints = await getPgConstraints(schema, pool);
    console.log(JSON.stringify(constraints[0]));

    const relationships = await getPgRelationships(schema, pool);
    console.log(JSON.stringify(relationships[0]));

    // Get the list of tables inside the schema.
    const tables = await pool.any(sql<InformationSchema>`
      SELECT table_name FROM information_schema.tables WHERE table_schema = ${schema} ORDER BY table_name`);

    if (tables.length === 0) {
      console.error(`No tables were found on schema ${schema}`);
      process.exit(1);
    }

    console.info(`Fetching enums`);
    // Get all the enum custom types definitions.
    const customEnums = await pool.any(sql<CustomEnumTypes>`
        SELECT t.typname as name, concat('"', string_agg(e.enumlabel, '","'), '"') AS value
        FROM pg_type t
        JOIN pg_enum e on t.oid = e.enumtypid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = ${schema}
        GROUP BY name;`);

    console.log("custom enums: ", JSON.stringify(customEnums, null, 2));
    const customEnumsTypesMap = customEnums.reduce(
      (acc, { name, value }) => ({
        ...acc,
        [name]: `z.enum([${value.split(",").sort().join(", ")}])`,
        // Add support for array of custom enum types. From PostgreSQL logs:
        //
        // > When you define a new base type, PostgreSQL automatically provides support for arrays
        // > of that type. The array type typically has the same name as the base type with the
        // > underscore character (_) prepended.
        [`_${name}`]: `z.array(z.enum([${value
          .split(",")
          .sort()
          .join(", ")}]))`,
      }),
      {}
    );

    const customZodTypesMap = customZodTypes.reduce((acc, pair) => {
      const [postgresType, zodValidator] = pair.split("=");
      return { ...acc, [postgresType]: zodValidator };
    }, {});

    // Create the types map
    const typesMap = createTypesMap({
      ...customEnumsTypesMap,
      ...customZodTypesMap,
    });

    // Create/Re-create the schema output folder
    if (clean) {
      await rm(output, { recursive: true }).catch(
        () => `output folder ${output} doesn't exist`
      );
    }
    await mkdir(output).catch(() => `output folder ${output} already exist`);

    // Run the correct strategy to generate the files.
    runWithStrategies({
      output,
      pool,
      strategies: Strategies.parse(strategies),
      tables,
      typesMap,
      constraints: [...constraints],
      columns: [...columns],
      relationships: [...relationships],
    });
  } catch (err) {
    console.error(err);
  }
};
// =========
// Functions
// =========
/**
 * Create the PGZod files depending on a strategy.
 *
 * The `write` strategy only creates types for inserting data on the table. It's configured as the
 * default options for backwards compatibility with previous versions of PGZod. This strategy is
 * useful of your access patterns on your table are the same for reads and writes.
 *
 * The `reqdwrite` strategy will make PGZod create two types for each table. One for `reads` and one
 * for `writes`. Each type will be suffixed with `Read` or `Write`. This strategy is useful when you
 * have access patterns on your table that differ between `reads` and `writes`. For example, if
 * you have a `column` with a default value, including it on `writes` is optional, but on `read` it
 * will always be there.
 */

async function runWithStrategies({
  output,
  pool,
  tables,
  typesMap,
  columns,
  constraints,
  relationships,
  strategies = ["write"],
}: StrategyOptions) {
  // Create the spinner
  console.info("Fetching tables metadata");

  // The index variable will hold all the lines for the ./index.ts file.
  const index: string[] = [];

  for (const table of tables.values()) {
    const { table_name } = table;

    // Set the current progress
    console.info(`Fetching columns metadata for table: ${table_name}`);
    const columnsIS = await pool.any(sql<ColumnsInformation>`
        SELECT is_generated, column_name, ordinal_position, column_default, is_nullable, data_type, udt_name
        FROM information_schema.columns
        WHERE table_name = ${table_name}
        ORDER BY ordinal_position
      `);

    console.log(relationships[0]);
    console.log(columns[0]);
    console.log(columnsIS);

    const template = [];
    // Remove editorconfig checks on auto-generated files.
    template.push(`import { z } from 'zod';\n`);

    // Add json parsing according to Zod documentation.
    // https://github.com/colinhacks/zod#json-type
    if (columnsIS.some((column) => column.udt_name === "jsonb")) {
      template.push(`type Literal = boolean | null | number | string;`);
      template.push(`type Json = Literal | { [key: string]: Json } | Json[];`);
      template.push(
        `const literalSchema = z.union([z.string(), z.number(), z.boolean(), z.null()]);`
      );
      template.push(`const jsonSchema: z.ZodSchema<Json> = z.lazy(() =>`);
      template.push(
        `  z.union([literalSchema, z.array(jsonSchema), z.record(jsonSchema)])`
      );
      template.push(`);\n`);
    }

    const name = pascalCase(table_name);
    template.push(`export const z${name}RecordStrict = {`);

    for (const column of columnsIS) {
      const name = column.column_name;
      let line = `${name}: `;

      const type = typesMap[column.udt_name];
      line += type;

      template.push(`  ${line},`);
    }

    template.push(`};\n`);

    const primaryKeys = constraints
      .filter(({ contype, relname }) => {
        return contype === "p" && relname === table_name;
      })
      .map(({ attname }) => attname);

    if (primaryKeys.length > 1)
      throw new Error(
        `Table ${table_name} has primary keys: ${primaryKeys.join(
          ", "
        )}. Does not support composite primary keys`
      );

    const primaryKey = primaryKeys[0] ?? `null`;

    const nullableFields = columnsIS
      .filter(({ is_nullable }) => is_nullable === "YES")
      .map(({ column_name }) => column_name);

    const getOptionalFields = (strat: StrategiesT[number]) =>
      columnsIS
        .filter(
          ({ column_name, is_nullable, is_generated, column_default }) => {
            if (strat === "read") return false;

            if (strat === "update") return column_name !== primaryKey;

            if (is_nullable === "YES") return true;

            return is_generated === "ALWAYS" || column_default !== null;
          }
        )
        .map(({ column_name }) => column_name);

    /**
     * Always create the types in the same order.
     */
    const strategiesSorted = STRATEGIES.filter((s) => strategies.includes(s));
    for (const strategy of strategiesSorted) {
      /**
       *  column/op            |   read           | write             | update
       *  -------------------------------------------------------------------------------
       *  null                 |   nullable       | optional/nullable | optional/nullable
       *  null     / default   |   nullable       | optional/nullable | optional/nullable
       *  not null / default   |   -              | optional          | optional
       *  not null             |   -              | -                 | optional
       *  primary key          |   -              | optional          | -
       */
      const optionalFields = getOptionalFields(strategy);

      const zname = (() => {
        if (strategy === "read") return name;
        return strategy === "write" ? `${name}Write` : `${name}Update`;
      })();

      template.push(
        nullableFields.length
          ? `export type ${zname}NullableFields = ${nullableFields
              .map((f) => `'${f}'`)
              .join(" | ")};\n`
          : `export type ${zname}NullableFields = never;\n`
      );

      template.push(
        optionalFields.length
          ? `export type ${zname}OptionalFields = ${optionalFields
              .map((f) => `'${f}'`)
              .join(" | ")};\n`
          : `export type ${zname}OptionalFields = never;\n`
      );

      template.push(`export const z${zname}Record = {`);
      template.push(`\t...z${name}RecordStrict,`);

      columnsIS
        .filter(
          ({ is_nullable, column_name }) =>
            is_nullable === "YES" || optionalFields.includes(column_name)
        )
        .map((column) => {
          const nullable = column.is_nullable === "YES";
          const optional = optionalFields.includes(column.column_name);

          let modifiedColumn = `z${name}RecordStrict.${column.column_name}`;
          if (nullable) modifiedColumn += ".nullable()";
          if (optional) modifiedColumn += ".optional()";
          return [column.column_name, modifiedColumn];
        })
        .forEach(([colname, typeModified]) =>
          template.push(`\t${colname}: ${typeModified},`)
        );

      template.push(`};\n`);

      console.log(nullableFields, optionalFields);
    }

    template.push(
      `export const z${name}Strict = z.object(z${name}RecordStrict);\n`
    );

    const indexImports = [`z${name}RecordStrict`];
    for (const strategy of strategiesSorted) {
      const zname = (() => {
        if (strategy === "read") return name;
        return strategy === "write" ? `${name}Write` : `${name}Update`;
      })();
      template.push(`export const z${zname} = z.object(z${zname}Record);\n`);
      indexImports.push(`z${zname}Record`);
    }

    template.push(
      `export type ${name}Strict = z.infer<typeof z${name}Strict>;\n`
    );
    const indexImportsTypes = [];
    for (const strategy of strategiesSorted) {
      const zname = (() => {
        if (strategy === "read") return name;
        return strategy === "write" ? `${name}Write` : `${name}Update`;
      })();
      template.push(`export type ${zname} = z.infer<typeof z${zname}>;\n`);

      indexImports.push(`z${zname}`);
      indexImportsTypes.push(zname);
    }

    const foreignKeys = relationships
      .filter(({ prelname }) => prelname === table_name)
      .map(({ frelname, pattname, fattname, conname }) => ({
        constraint_name: conname,
        table: frelname,
        pkey: pattname,
        fkey: fattname,
      }));

    if (
      foreignKeys.length >
      new Set(foreignKeys.map(({ constraint_name }) => constraint_name)).size
    )
      throw new Error(
        "Duplicate constraint name, does not support relationships foreign keys"
      );

    const columnsJnt = columnsIS
      .map(({ column_name }) => `"${column_name}"`)
      .join(", ");

    /**
     * Write a const that provides just const objects.
     */
    template.push(`export const ${table_name} = {`);
    template.push(`  columns: [${columnsJnt}],`);
    template.push(`  primaryKey: "${primaryKey}",`);
    template.push(`  foreignKeys: {`);
    foreignKeys.forEach(({ table, pkey, fkey }) => {
      template.push(`    ${pkey}: { table: "${table}", column: "${fkey}" },`);
    });
    template.push(`  },`);
    template.push(`  ops: {`);
    template.push(`    strict: {`);
    template.push(`      record: z${name}RecordStrict,`);
    template.push(`      z: z${name}Strict,`);
    template.push(`      $type: null as unknown as ${name}Strict,`);
    template.push(`    },`);

    const nullableFieldsJnt = nullableFields
      .map((field) => `"${field}"`)
      .join(", ");
    strategiesSorted.forEach((strategy) => {
      const zname = (() => {
        if (strategy === "read") return name;
        return strategy === "write" ? `${name}Write` : `${name}Update`;
      })();

      const optionalFieldsJnt = getOptionalFields(strategy)
        .map((field) => `"${field}"`)
        .join(", ");

      template.push(`    ${strategy}: {`);
      template.push(`      nullable: [${nullableFieldsJnt}],`);
      template.push(`      optional: [${optionalFieldsJnt}],`);
      template.push(`      record: z${zname}Record,`);
      template.push(`      z: z${zname},`);
      template.push(`      $type: null as unknown as ${zname},`);
      template.push(`    },`);
    });
    template.push(`  },`);
    template.push(`} as const;\n`);

    template.push(`export type ${name}Types = {`);
    template.push(`  strict: ${name}Strict;`);
    strategiesSorted.forEach((strat) => {
      const zname = (() => {
        if (strat === "read") return name;
        return strat === "write" ? `${name}Write` : `${name}Update`;
      })();

      template.push(`  ${strat}: ${zname};`);
    });
    template.push(`};`);

    const file = camelCase(name);
    await writeFile(join(output, `${file}.ts`), template.join("\n"));

    const indexImportTypes = indexImportsTypes.join(", ");
    const indexImport = indexImports.join(", ");

    index.push(
      `export type { ${indexImportTypes}, ${name}Types } from './${file}.js';`
    );
    index.push(`export { ${table_name}, ${indexImport} } from './${file}.js';`);
    index.push(`import type { ${name}Types } from './${file}.js';`);
    index.push(`import { ${table_name} } from './${file}.js';\n`);
  }

  index.push(`export const tables = {`);
  tables.forEach(({ table_name }) => {
    index.push(`  ${table_name},`);
  });
  index.push(`};\n`);

  index.push(`export type TableTypes = {`);
  tables.forEach(({ table_name }) => {
    index.push(`  ${table_name}: ${pascalCase(table_name)}Types,`);
  });
  index.push(`};`);

  console.info("Writing index file");
  await writeFile(join(output, `index.ts`), [...index, "\n"].join("\n"));

  console.info("Done");
}

/**
 * Returns a PostgreSQL to Zod types map.
 * @param type - PostgreSQL data type.
 */
function createTypesMap(customZodTypes: Record<string, string>) {
  /**
   * Map that translate PostgreSQL data types to Zod functions.
   */
  const ZOD_TYPES_OVERRIDE: Record<string, string> = {
    bool: `z.boolean()`,
    bpchar: `z.string()`,
    citext: `z.string()`,
    // TODO: Find a better way to handle dates.
    date: `z.string()`,
    float8: `z.number()`,
    int4: `z.number().int()`,
    jsonb: `jsonSchema`,
    numeric: `z.number()`,
    text: `z.string()`,
    // TODO: Find a better way to handle dates.
    timestamp: `z.date()`,
    timestamptz: `z.date()`,
    uuid: "z.string().uuid()",
    varchar: `z.string()`,
    interval: `z.number()`,
  };
  const map = { ...ZOD_TYPES_OVERRIDE, ...customZodTypes };
  const proxy = new Proxy(map, {
    get: (object, prop: string) =>
      prop in object ? object[prop] : `z.${prop}()`,
  });
  return proxy;
}
// =====
// Types
// =====
/**
 * Represents the output of the information_schema.tables table.
 */
type InformationSchema = {
  table_name: string;
};
/**
 * Represents the output of a query to get the list of custom enum types.
 */
type CustomEnumTypes = {
  /**
   * Name of the custom enum type.
   */
  name: string;
  /**
   * List of valid enum values as a concatenated list of string separated by a comma (,).
   */
  value: string;
};
/**
 * Represents the output of the information_schema.colums table.
 */
type ColumnsInformation = {
  column_default: string | null;
  column_name: string;
  data_type: string;
  is_generated: "NEVER" | "ALWAYS";
  is_nullable: "YES" | "NO";
  ordinal_position: number;
  udt_name: string;
};
/**
 * Represents the options needed to run a PGZod strategy.
 */
type StrategyOptions = {
  output: string;
  pool: DatabasePool;
  strategies?: StrategiesT;
  tables: readonly InformationSchema[];
  typesMap: { [key: string]: string };
  columns: PgColumn[];
  constraints: PgConstraint[];
  relationships: PgRelationship[];
};
// =================
// Standalone Module
// =================
if (require.main === module) {
  yargs(hideBin(process.argv))
    .env(true)
    .command({
      command: "$0",
      describe,
      builder,
      handler,
    })
    .parseAsync();
}
