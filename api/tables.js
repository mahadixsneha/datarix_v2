const { getDb, runRaw } = require('../lib/db');
const { requireAuth, cors } = require('../lib/auth');
const { safeName, userTableName, safeType } = require('../lib/sanitize');
const { checkLimit } = require('../lib/plan');

// ===== TEMPLATES =====
const TEMPLATES = {
  earning_app: {
    name: 'Earning App', icon: 'payments', color: '#3ecf8e',
    description: 'Tasks, withdrawals, transactions, referrals',
    tables: [
      { name: 'ea_settings', type: 'sql', columns: [
        {name:'key',type:'TEXT'},{name:'value',type:'TEXT'}
      ], sample: [
        {key:'minWithdraw',value:'10'},{key:'referralBonus',value:'1'},
        {key:'referralRequirement',value:'3'},{key:'paymentMethods',value:'bKash,Nagad,Rocket'},
        {key:'announcementText',value:''},{key:'announcementActive',value:'false'}
      ]},
      { name: 'ea_tasks', type: 'sql', columns: [
        {name:'title',type:'TEXT'},{name:'description',type:'TEXT'},
        {name:'reward',type:'NUMERIC'},{name:'url',type:'TEXT'},
        {name:'type',type:'TEXT'},{name:'is_active',type:'BOOLEAN'},
        {name:'created_at',type:'TIMESTAMP'}
      ], sample: [
        {title:'Watch Video',description:'Watch and earn',reward:5,url:'https://youtube.com',type:'video',is_active:true,created_at:new Date().toISOString()}
      ]},
      { name: 'ea_transactions', type: 'sql', columns: [
        {name:'user_id',type:'TEXT'},{name:'user_name',type:'TEXT'},
        {name:'type',type:'TEXT'},{name:'amount',type:'NUMERIC'},
        {name:'description',type:'TEXT'},{name:'status',type:'TEXT'},
        {name:'created_at',type:'TIMESTAMP'}
      ]},
      { name: 'ea_withdrawals', type: 'sql', columns: [
        {name:'user_id',type:'TEXT'},{name:'user_name',type:'TEXT'},
        {name:'method',type:'TEXT'},{name:'account_number',type:'TEXT'},
        {name:'amount',type:'NUMERIC'},{name:'status',type:'TEXT'},
        {name:'note',type:'TEXT'},{name:'created_at',type:'TIMESTAMP'}
      ]},
      { name: 'ea_announcements', type: 'sql', columns: [
        {name:'text',type:'TEXT'},{name:'is_active',type:'BOOLEAN'},{name:'created_at',type:'TIMESTAMP'}
      ]}
    ]
  },
  blog_cms: {
    name: 'Blog / CMS', icon: 'article', color: '#6366f1',
    description: 'Posts, categories, comments, subscribers',
    tables: [
      { name: 'blog_categories', type: 'sql', columns: [
        {name:'name',type:'TEXT'},{name:'slug',type:'TEXT'},{name:'description',type:'TEXT'},{name:'created_at',type:'TIMESTAMP'}
      ], sample: [
        {name:'Technology',slug:'technology',description:'Tech articles',created_at:new Date().toISOString()},
        {name:'News',slug:'news',description:'Latest news',created_at:new Date().toISOString()}
      ]},
      { name: 'blog_posts', type: 'sql', columns: [
        {name:'title',type:'TEXT'},{name:'slug',type:'TEXT'},{name:'content',type:'TEXT'},
        {name:'excerpt',type:'TEXT'},{name:'thumbnail',type:'TEXT'},{name:'category_id',type:'INTEGER'},
        {name:'author',type:'TEXT'},{name:'tags',type:'TEXT'},{name:'status',type:'TEXT'},
        {name:'views',type:'INTEGER'},{name:'created_at',type:'TIMESTAMP'},{name:'updated_at',type:'TIMESTAMP'}
      ], sample: [
        {title:'Welcome!',slug:'welcome',content:'Hello world!',excerpt:'Hello world!',category_id:1,author:'Admin',tags:'welcome',status:'published',views:0,created_at:new Date().toISOString(),updated_at:new Date().toISOString()}
      ]},
      { name: 'blog_comments', type: 'sql', columns: [
        {name:'post_id',type:'INTEGER'},{name:'author_name',type:'TEXT'},
        {name:'author_email',type:'TEXT'},{name:'content',type:'TEXT'},
        {name:'status',type:'TEXT'},{name:'created_at',type:'TIMESTAMP'}
      ]},
      { name: 'blog_subscribers', type: 'sql', columns: [
        {name:'email',type:'TEXT'},{name:'name',type:'TEXT'},{name:'is_active',type:'BOOLEAN'},{name:'created_at',type:'TIMESTAMP'}
      ]}
    ]
  },
  ecommerce: {
    name: 'E-commerce', icon: 'shopping_cart', color: '#f4a261',
    description: 'Products, categories, orders, customers, reviews',
    tables: [
      { name: 'shop_categories', type: 'sql', columns: [
        {name:'name',type:'TEXT'},{name:'slug',type:'TEXT'},{name:'image',type:'TEXT'},{name:'is_active',type:'BOOLEAN'}
      ], sample: [
        {name:'Electronics',slug:'electronics',image:'',is_active:true},
        {name:'Clothing',slug:'clothing',image:'',is_active:true}
      ]},
      { name: 'shop_products', type: 'sql', columns: [
        {name:'name',type:'TEXT'},{name:'slug',type:'TEXT'},{name:'description',type:'TEXT'},
        {name:'price',type:'NUMERIC'},{name:'sale_price',type:'NUMERIC'},{name:'stock',type:'INTEGER'},
        {name:'category_id',type:'INTEGER'},{name:'images',type:'TEXT'},{name:'sku',type:'TEXT'},
        {name:'is_active',type:'BOOLEAN'},{name:'created_at',type:'TIMESTAMP'}
      ], sample: [
        {name:'Sample Product',slug:'sample',description:'A great product',price:99.99,sale_price:79.99,stock:100,category_id:1,images:'',sku:'SKU001',is_active:true,created_at:new Date().toISOString()}
      ]},
      { name: 'shop_customers', type: 'sql', columns: [
        {name:'name',type:'TEXT'},{name:'email',type:'TEXT'},{name:'phone',type:'TEXT'},
        {name:'address',type:'TEXT'},{name:'city',type:'TEXT'},{name:'total_orders',type:'INTEGER'},
        {name:'total_spent',type:'NUMERIC'},{name:'created_at',type:'TIMESTAMP'}
      ]},
      { name: 'shop_orders', type: 'sql', columns: [
        {name:'customer_id',type:'INTEGER'},{name:'customer_name',type:'TEXT'},{name:'customer_email',type:'TEXT'},
        {name:'customer_phone',type:'TEXT'},{name:'items',type:'TEXT'},{name:'subtotal',type:'NUMERIC'},
        {name:'shipping',type:'NUMERIC'},{name:'total',type:'NUMERIC'},{name:'status',type:'TEXT'},
        {name:'payment_method',type:'TEXT'},{name:'payment_status',type:'TEXT'},
        {name:'address',type:'TEXT'},{name:'note',type:'TEXT'},{name:'created_at',type:'TIMESTAMP'}
      ]},
      { name: 'shop_reviews', type: 'sql', columns: [
        {name:'product_id',type:'INTEGER'},{name:'customer_name',type:'TEXT'},
        {name:'rating',type:'INTEGER'},{name:'comment',type:'TEXT'},
        {name:'is_approved',type:'BOOLEAN'},{name:'created_at',type:'TIMESTAMP'}
      ]}
    ]
  },
  todo_manager: {
    name: 'Todo / Task Manager', icon: 'checklist', color: '#a78bfa',
    description: 'Projects, tasks, labels — team task management',
    tables: [
      { name: 'todo_projects', type: 'sql', columns: [
        {name:'name',type:'TEXT'},{name:'description',type:'TEXT'},{name:'color',type:'TEXT'},{name:'is_archived',type:'BOOLEAN'},{name:'created_at',type:'TIMESTAMP'}
      ], sample: [
        {name:'Personal',description:'Personal tasks',color:'#6366f1',is_archived:false,created_at:new Date().toISOString()},
        {name:'Work',description:'Work tasks',color:'#3ecf8e',is_archived:false,created_at:new Date().toISOString()}
      ]},
      { name: 'todo_tasks', type: 'sql', columns: [
        {name:'title',type:'TEXT'},{name:'description',type:'TEXT'},{name:'project_id',type:'INTEGER'},
        {name:'priority',type:'TEXT'},{name:'status',type:'TEXT'},{name:'due_date',type:'DATE'},
        {name:'label',type:'TEXT'},{name:'assigned_to',type:'TEXT'},
        {name:'created_at',type:'TIMESTAMP'},{name:'completed_at',type:'TIMESTAMP'}
      ], sample: [
        {title:'Welcome task!',description:'Your first task',project_id:1,priority:'medium',status:'pending',label:'example',created_at:new Date().toISOString()}
      ]},
      { name: 'todo_labels', type: 'sql', columns: [
        {name:'name',type:'TEXT'},{name:'color',type:'TEXT'},{name:'created_at',type:'TIMESTAMP'}
      ], sample: [
        {name:'urgent',color:'#f25f5c',created_at:new Date().toISOString()},
        {name:'important',color:'#f4a261',created_at:new Date().toISOString()}
      ]}
    ]
  },
  contact_form: {
    name: 'Contact Form / Leads', icon: 'contact_mail', color: '#f25f5c',
    description: 'Leads, newsletter, feedback collector',
    tables: [
      { name: 'cf_leads', type: 'sql', columns: [
        {name:'name',type:'TEXT'},{name:'email',type:'TEXT'},{name:'phone',type:'TEXT'},
        {name:'subject',type:'TEXT'},{name:'message',type:'TEXT'},{name:'source',type:'TEXT'},
        {name:'status',type:'TEXT'},{name:'created_at',type:'TIMESTAMP'}
      ]},
      { name: 'cf_newsletter', type: 'sql', columns: [
        {name:'email',type:'TEXT'},{name:'name',type:'TEXT'},{name:'is_active',type:'BOOLEAN'},{name:'created_at',type:'TIMESTAMP'}
      ]},
      { name: 'cf_feedback', type: 'sql', columns: [
        {name:'user_name',type:'TEXT'},{name:'email',type:'TEXT'},{name:'rating',type:'INTEGER'},
        {name:'category',type:'TEXT'},{name:'message',type:'TEXT'},{name:'status',type:'TEXT'},
        {name:'created_at',type:'TIMESTAMP'}
      ]},
      { name: 'cf_settings', type: 'sql', columns: [
        {name:'key',type:'TEXT'},{name:'value',type:'TEXT'}
      ], sample: [
        {key:'notify_email',value:''},{key:'success_message',value:'Thank you!'},{key:'spam_protection',value:'true'}
      ]}
    ]
  },
  restaurant: {
    name: 'Restaurant Menu', icon: 'restaurant', color: '#fb923c',
    description: 'Menu, categories, orders, reservations, tables',
    tables: [
      { name: 'rest_categories', type: 'sql', columns: [
        {name:'name',type:'TEXT'},{name:'description',type:'TEXT'},{name:'image',type:'TEXT'},
        {name:'sort_order',type:'INTEGER'},{name:'is_active',type:'BOOLEAN'}
      ], sample: [
        {name:'Starters',description:'Appetizers',image:'',sort_order:1,is_active:true},
        {name:'Main Course',description:'Main dishes',image:'',sort_order:2,is_active:true},
        {name:'Desserts',description:'Sweet endings',image:'',sort_order:3,is_active:true},
        {name:'Drinks',description:'Beverages',image:'',sort_order:4,is_active:true}
      ]},
      { name: 'rest_menu', type: 'sql', columns: [
        {name:'name',type:'TEXT'},{name:'description',type:'TEXT'},{name:'price',type:'NUMERIC'},
        {name:'category_id',type:'INTEGER'},{name:'image',type:'TEXT'},{name:'is_veg',type:'BOOLEAN'},
        {name:'is_spicy',type:'BOOLEAN'},{name:'is_available',type:'BOOLEAN'},
        {name:'calories',type:'INTEGER'},{name:'sort_order',type:'INTEGER'}
      ], sample: [
        {name:'Spring Rolls',description:'Crispy rolls',price:120,category_id:1,image:'',is_veg:true,is_spicy:false,is_available:true,calories:180,sort_order:1},
        {name:'Chicken Burger',description:'Juicy burger',price:250,category_id:2,image:'',is_veg:false,is_spicy:false,is_available:true,calories:450,sort_order:1}
      ]},
      { name: 'rest_orders', type: 'sql', columns: [
        {name:'table_number',type:'TEXT'},{name:'customer_name',type:'TEXT'},
        {name:'customer_phone',type:'TEXT'},{name:'items',type:'TEXT'},
        {name:'total',type:'NUMERIC'},{name:'status',type:'TEXT'},
        {name:'order_type',type:'TEXT'},{name:'note',type:'TEXT'},{name:'created_at',type:'TIMESTAMP'}
      ]},
      { name: 'rest_reservations', type: 'sql', columns: [
        {name:'customer_name',type:'TEXT'},{name:'customer_phone',type:'TEXT'},
        {name:'customer_email',type:'TEXT'},{name:'date',type:'DATE'},
        {name:'time',type:'TEXT'},{name:'guests',type:'INTEGER'},
        {name:'status',type:'TEXT'},{name:'note',type:'TEXT'},{name:'created_at',type:'TIMESTAMP'}
      ]}
    ]
  },
  custom_collection: {
    name: 'Custom Collection', icon: 'data_object', color: '#22d3ee',
    description: 'MongoDB-style schema-less JSONB collection',
    tables: [
      { name: 'my_collection', type: 'collection', columns: [] }
    ]
  }
};

module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || (req.method === 'GET' ? 'list' : 'create');
  const sql = getDb();

  // PUBLIC: list templates
  if (req.method === 'GET' && action === 'templates') {
    const list = Object.entries(TEMPLATES).map(([key, t]) => ({
      key, name: t.name, description: t.description, icon: t.icon, color: t.color,
      table_count: t.tables.length,
      tables: t.tables.map(tb => ({ name: tb.name, type: tb.type || 'sql', columns: tb.columns.length }))
    }));
    return res.json({ templates: list });
  }

  const user = requireAuth(req, res); if (!user) return;

  // LIST tables
  if (action === 'list') {
    try {
      const tables = await sql`SELECT id, table_name, physical_name, table_type, schema, is_public, created_at FROM tb_table_registry WHERE user_id = ${user.id} ORDER BY created_at DESC`;
      const result = [];
      for (const t of tables) {
        let rowCount = 0;
        try { const cnt = await runRaw(`SELECT COUNT(*) as count FROM "${t.physical_name}"`); rowCount = parseInt(cnt[0].count) || 0; } catch (e) {}
        result.push({ ...t, row_count: rowCount });
      }
      return res.json({ tables: result });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  // CREATE table (SQL or Collection)
  if (action === 'create') {
    const limit = await checkLimit(user.id, 'tables');
    if (!limit.allowed) return res.status(400).json({ error: limit.message });
    const { table_name, columns, raw_sql, table_type = 'sql' } = req.body || {};
    if (!table_name) return res.status(400).json({ error: 'Table name required' });
    const cleanName = safeName(table_name);
    const physicalName = userTableName(user.id, table_name);
    try {
      const existing = await sql`SELECT id FROM tb_table_registry WHERE user_id = ${user.id} AND table_name = ${cleanName}`;
      if (existing.length) return res.status(409).json({ error: 'Name already exists' });

      let createSQL, schemaJson;

      if (table_type === 'collection') {
        // MongoDB-style JSONB collection
        createSQL = `CREATE TABLE "${physicalName}" (id SERIAL PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}', created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())`;
        schemaJson = JSON.stringify({ type: 'collection' });
      } else if (raw_sql) {
        createSQL = raw_sql.replace(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+/i, `CREATE TABLE "${physicalName}"`);
        schemaJson = JSON.stringify({ raw: true, sql: raw_sql });
      } else {
        const colDefs = (columns || []).filter(c => c.name && c.type).map(c => {
          let def = `"${safeName(c.name)}" ${safeType(c.type)}`;
          if (c.not_null) def += ' NOT NULL';
          if (c.unique) def += ' UNIQUE';
          if (c.default !== undefined && c.default !== '') def += ` DEFAULT '${c.default}'`;
          return def;
        }).join(', ');
        createSQL = colDefs ? `CREATE TABLE "${physicalName}" (id SERIAL PRIMARY KEY, ${colDefs})` : `CREATE TABLE "${physicalName}" (id SERIAL PRIMARY KEY)`;
        schemaJson = JSON.stringify({ raw: false, type: 'sql', columns: columns || [] });
      }

      await runRaw(createSQL);
      const rows = await sql`INSERT INTO tb_table_registry (user_id, table_name, physical_name, table_type, schema) VALUES (${user.id}, ${cleanName}, ${physicalName}, ${table_type}, ${schemaJson}) RETURNING *`;
      await sql`INSERT INTO tb_activity (user_id, action, details) VALUES (${user.id}, 'create_table', ${`Created ${table_type}: ${cleanName}`})`;
      return res.status(201).json({ table: rows[0] });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  // APPLY TEMPLATE
  if (action === 'apply_template') {
    const { template_key, with_sample } = req.body || {};
    const tpl = TEMPLATES[template_key];
    if (!tpl) return res.status(404).json({ error: 'Template not found' });

    const results = [];
    for (const tbl of tpl.tables) {
      const limit = await checkLimit(user.id, 'tables');
      if (!limit.allowed) { results.push({ name: tbl.name, error: 'Plan limit reached' }); continue; }
      const cleanName = safeName(tbl.name);
      const physName = userTableName(user.id, tbl.name);
      try {
        const existing = await sql`SELECT id FROM tb_table_registry WHERE user_id = ${user.id} AND table_name = ${cleanName}`;
        if (existing.length) { results.push({ name: tbl.name, status: 'skipped', reason: 'already exists' }); continue; }

        let createSQL;
        if (tbl.type === 'collection') {
          createSQL = `CREATE TABLE "${physName}" (id SERIAL PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}', created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())`;
        } else {
          const colDefs = tbl.columns.map(c => `"${safeName(c.name)}" ${safeType(c.type)}`).join(', ');
          createSQL = colDefs ? `CREATE TABLE "${physName}" (id SERIAL PRIMARY KEY, ${colDefs})` : `CREATE TABLE "${physName}" (id SERIAL PRIMARY KEY)`;
        }
        await runRaw(createSQL);
        const schemaJson = JSON.stringify({ type: tbl.type || 'sql', columns: tbl.columns, template: template_key });
        await sql`INSERT INTO tb_table_registry (user_id, table_name, physical_name, table_type, schema) VALUES (${user.id}, ${cleanName}, ${physName}, ${tbl.type || 'sql'}, ${schemaJson})`;

        // Insert sample data
        if (with_sample && tbl.sample && tbl.sample.length > 0) {
          for (const row of tbl.sample) {
            const entries = Object.entries(row);
            const keys = entries.map(([k]) => `"${safeName(k)}"`).join(', ');
            const vals = entries.map(([, v]) => v === '' ? null : v);
            const phs = vals.map((_, i) => `$${i + 1}`).join(', ');
            try { await runRaw(`INSERT INTO "${physName}" (${keys}) VALUES (${phs})`, vals); } catch (e) {}
          }
        }
        results.push({ name: tbl.name, status: 'created', type: tbl.type || 'sql' });
      } catch (e) { results.push({ name: tbl.name, error: e.message }); }
    }
    await sql`INSERT INTO tb_activity (user_id, action, details) VALUES (${user.id}, 'apply_template', ${`Applied template: ${tpl.name}`})`;
    return res.json({ results, template: tpl.name });
  }

  // DELETE
  if (action === 'delete') {
    const { table_name } = req.body || {};
    try {
      const rows = await sql`SELECT * FROM tb_table_registry WHERE user_id = ${user.id} AND table_name = ${table_name}`;
      if (!rows.length) return res.status(404).json({ error: 'Table not found' });
      await runRaw(`DROP TABLE IF EXISTS "${rows[0].physical_name}"`);
      await sql`DELETE FROM tb_table_registry WHERE id = ${rows[0].id}`;
      return res.json({ success: true });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  // RENAME
  if (action === 'rename') {
    const { table_name, new_name } = req.body || {};
    try {
      const rows = await sql`SELECT * FROM tb_table_registry WHERE user_id = ${user.id} AND table_name = ${table_name}`;
      if (!rows.length) return res.status(404).json({ error: 'Table not found' });
      await sql`UPDATE tb_table_registry SET table_name = ${safeName(new_name)} WHERE id = ${rows[0].id}`;
      return res.json({ success: true });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  // DUPLICATE
  if (action === 'duplicate') {
    const limit = await checkLimit(user.id, 'tables');
    if (!limit.allowed) return res.status(400).json({ error: limit.message });
    const { table_name } = req.body || {};
    try {
      const rows = await sql`SELECT * FROM tb_table_registry WHERE user_id = ${user.id} AND table_name = ${table_name}`;
      if (!rows.length) return res.status(404).json({ error: 'Table not found' });
      const newName = safeName(table_name + '_copy');
      const newPhys = userTableName(user.id, newName + '_' + Date.now());
      await runRaw(`CREATE TABLE "${newPhys}" AS SELECT * FROM "${rows[0].physical_name}"`);
      const nr = await sql`INSERT INTO tb_table_registry (user_id, table_name, physical_name, table_type, schema) VALUES (${user.id}, ${newName}, ${newPhys}, ${rows[0].table_type || 'sql'}, ${rows[0].schema}) RETURNING *`;
      return res.status(201).json({ table: nr[0] });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  // ADD COLUMN
  if (action === 'addcol') {
    const { table_name, column_name, column_type, not_null, default_val } = req.body || {};
    try {
      const rows = await sql`SELECT * FROM tb_table_registry WHERE user_id = ${user.id} AND table_name = ${table_name}`;
      if (!rows.length) return res.status(404).json({ error: 'Table not found' });
      if (rows[0].table_type === 'collection') return res.status(400).json({ error: 'Collections use JSONB — no column management needed' });
      let alterSQL = `ALTER TABLE "${rows[0].physical_name}" ADD COLUMN "${safeName(column_name)}" ${safeType(column_type)}`;
      if (not_null && default_val !== undefined) alterSQL += ` NOT NULL DEFAULT '${default_val}'`;
      else if (default_val !== undefined && default_val !== '') alterSQL += ` DEFAULT '${default_val}'`;
      await runRaw(alterSQL);
      return res.json({ success: true });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  // DROP COLUMN
  if (action === 'dropcol') {
    const { table_name, column_name } = req.body || {};
    try {
      const rows = await sql`SELECT * FROM tb_table_registry WHERE user_id = ${user.id} AND table_name = ${table_name}`;
      if (!rows.length) return res.status(404).json({ error: 'Table not found' });
      await runRaw(`ALTER TABLE "${rows[0].physical_name}" DROP COLUMN IF EXISTS "${safeName(column_name)}"`);
      return res.json({ success: true });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  // GET COLUMNS
  if (action === 'columns') {
    const { table_name } = req.query;
    try {
      const rows = await sql`SELECT * FROM tb_table_registry WHERE user_id = ${user.id} AND table_name = ${table_name}`;
      if (!rows.length) return res.status(404).json({ error: 'Table not found' });
      const cols = await sql`SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = ${rows[0].physical_name} ORDER BY ordinal_position`;
      return res.json({ columns: cols, table_type: rows[0].table_type });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  // CSV EXPORT
  if (action === 'export') {
    const { table_name } = req.query;
    try {
      const rows = await sql`SELECT * FROM tb_table_registry WHERE user_id = ${user.id} AND table_name = ${table_name}`;
      if (!rows.length) return res.status(404).json({ error: 'Table not found' });
      const data = await runRaw(`SELECT * FROM "${rows[0].physical_name}" ORDER BY id`);
      if (!data.length) return res.json({ csv: '' });
      const headers = Object.keys(data[0]).join(',');
      const csvRows = data.map(row => Object.values(row).map(v => v === null ? '' : `"${String(v).replace(/"/g, '""')}"`).join(','));
      return res.json({ csv: [headers, ...csvRows].join('\n'), filename: `${table_name}.csv` });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  // CSV IMPORT
  if (action === 'import') {
    const { table_name, csv_data } = req.body || {};
    try {
      const rows = await sql`SELECT * FROM tb_table_registry WHERE user_id = ${user.id} AND table_name = ${table_name}`;
      if (!rows.length) return res.status(404).json({ error: 'Table not found' });
      const lines = csv_data.trim().split('\n');
      const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
      let imported = 0;
      for (let i = 1; i < lines.length; i++) {
        const vals = lines[i].split(',').map(v => v.trim().replace(/^"|"$/g, '') || null);
        const filteredHeaders = headers.filter(h => h !== 'id');
        const filteredVals = headers.map((h, idx) => h === 'id' ? null : vals[idx]).filter((_, idx) => headers[idx] !== 'id');
        if (!filteredHeaders.length) continue;
        const keys = filteredHeaders.map(h => `"${safeName(h)}"`).join(', ');
        const phs = filteredVals.map((_, pi) => `$${pi + 1}`).join(', ');
        try { await runRaw(`INSERT INTO "${rows[0].physical_name}" (${keys}) VALUES (${phs})`, filteredVals); imported++; } catch (e) {}
      }
      return res.json({ success: true, imported });
    } catch (e) { return res.status(500).json({ error: e.message }); }
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
