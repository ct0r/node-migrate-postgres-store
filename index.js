const postgres = require("postgres");

class PostgresStore {
  async init() {
    const sql = postgres(process.env.MIGRATE_POSTGRES_STORE_URL);

    await sql`
        CREATE TABLE IF NOT EXISTS migrations
        (
          title varchar(255) CONSTRAINT migrations_pk PRIMARY KEY,
          timestamp bigint,
          description text,
          last bool NOT NULL
        );
      `;

    return sql;
  }

  async load(cb) {
    try {
      const sql = await this.init();

      const records = await sql`
        SELECT * FROM migrations
      `;

      const set = records.reduce(
        (set, { title, timestamp, description, last }) => ({
          lastRun: set.lastRun || (last && title),
          migrations: [
            ...set.migrations,
            {
              title,
              timestamp,
              description,
            },
          ],
        }),
        { migrations: [] }
      );

      cb(null, set);
    } catch (err) {
      cb(err);
    }
  }

  async save({ lastRun, migrations }, cb) {
    try {
      const sql = await this.init();

      const records = migrations.map(({ title, timestamp, description }) => ({
        title,
        timestamp,
        description,
        last: title === lastRun,
      }));

      await sql.begin(async (sql) => {
        await sql`
          TRUNCATE TABLE migrations
        `;

        await sql`
          INSERT INTO migrations ${sql(records)}
        `;
      });

      cb();
    } catch (err) {
      cb(err);
    }
  }
}

module.exports = { PostgresStore };
