import psycopg
from flask import Flask
from flask_restx import Api, Resource, fields
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
api = Api(
    app,
    version="1.0",
    title="Contact API",
    description="A simple Contact API",
)

ns = api.namespace("contacts", description="CONTACT operations")

address = api.model(
    "Addresses", {
        "address_id": fields.Integer(readonly=True, description="The address unique identifier"),
        "contact_id": fields.Integer(required=True, description="The contact key"),
        "country": fields.String(required=False, description="The contact country"),
        "title": fields.String(required=False, description="The contact title"),
        "postal_code": fields.String(required=False, description="The address postal code"),
        "phone": fields.String(required=False, description="The address phone number"),
        "province": fields.String(required=False, description="The address province"),
        "city": fields.String(required=False, description="The address city"),
        "street1": fields.String(required=False, description="The address street 1"),
        "street2": fields.String(required=False, description="The address street 2"),
        "email": fields.String(required=False, description="The address email"),

    }
)

contact = api.model(
    "Contact",
    {
        "contact_id": fields.Integer(readonly=True, description="The contact unique identifier"),
        "first_name": fields.String(required=True, description="The contact first name"),
        "last_name": fields.String(required=True, description="The contact last name"),
        "middle_name": fields.String(required=False, description="The contact middle name"),
        "addresses": fields.List(fields.Nested(address))
    }
)

DB_HOST = "localhost"
DB_USER = "pete"
DB_PASS = "pete"


class ContactDAO(object):

    def create_tables(self, contact_table_name: str, address_table_name: str, drop: bool = False):
        create_contact_sql = f"CREATE TABLE IF NOT EXISTS {contact_table_name} ("
        create_contact_sql += '"contact_id" serial PRIMARY KEY,'
        create_contact_sql += '"birth_date" date,'
        create_contact_sql += '"first_name" character varying(50),'
        create_contact_sql += '"last_name" character varying(50),'
        create_contact_sql += '"middle_name" character varying(50))'

        create_address_sql = f"CREATE TABLE IF NOT EXISTS {address_table_name} ("
        create_address_sql += '"address_id" serial PRIMARY KEY,'
        create_address_sql += 'country character varying(6),'
        create_address_sql += 'title character varying(5),'
        create_address_sql += 'postal_code character varying(15),'
        create_address_sql += 'phone character varying(15),'
        create_address_sql += 'province character varying(20),'
        create_address_sql += 'city character varying(50),'
        create_address_sql += 'street1 character varying(100),'
        create_address_sql += 'street2 character varying(100),'
        create_address_sql += 'email character varying(250),'
        create_address_sql += '"contact_id" integer)'

        with psycopg.connect("user=" + DB_USER + " password=" + DB_PASS + " host=" + DB_HOST) as conn:
            # Open a cursor to perform database operations
            with conn.cursor() as cur:
                if drop:
                    # remove tables if found
                    cur.execute("DROP TABLE IF EXISTS " + address_table_name)
                    cur.execute("DROP TABLE IF EXISTS " + contact_table_name)
                # Execute a command: this creates a new table
                cur.execute(create_contact_sql)
                cur.execute(create_address_sql)

    def __init__(self):
        self.create_tables("aaa", "bbb", drop=True)
        self.counter = 0
        self.contacts = []

    def get(self, contact_id):
        for contact in self.contacts:
            if contact["contact_id"] == contact_id:
                return contact
        api.abort(404, "Contact {} doesn't exist".format(contact_id))

    def create(self, data):
        contact = data
        contact["contact_id"] = self.counter = self.counter + 1
        self.contacts.append(contact)
        return contact

    def update(self, contact_id, data):
        contact = self.get(contact_id)
        contact.update(data)
        return contact

    def delete(self, contact_id):
        contact = self.get(contact_id)
        self.contacts.remove(contact)

contact_dao = ContactDAO()
contact_dao.create({"first_name": "Build an API", "last_name": "SSS", "middle_name": "PPP"})
contact_dao.create({"first_name": "?????", "last_name": "AAA", "middle_name": "TTT"})
contact_dao.create({"first_name": "profit!", "last_name": "BBB", "middle_name": "QQQ"})

@ns.route("/")
class ContactList(Resource):
    """Shows a list of all contacts, and lets you POST to add new contacts"""

    @ns.doc("list_contacts")
    @ns.marshal_list_with(contact)
    def get(self):
        """List all contacts"""
        return contact_dao.contacts

    @ns.doc("create_contact")
    @ns.expect(contact)
    @ns.marshal_with(contact, code=201)
    def post(self):
        """Create a new contact"""
        return contact_dao.create(api.payload), 201

@ns.route("/<string:contact_id>")
@ns.response(404, "Contact not found")
@ns.param("contact_id", "The contact identifier")
class Contact(Resource):
    """Show a single contact item and lets you delete them"""

    @ns.doc("get_contact")
    @ns.marshal_with(contact)
    def get(self, contact_id):
        """Fetch a given resource"""
        return contact_dao.get(contact_id)

    @ns.doc("delete_contacts")
    @ns.response(204, "Contact deleted")
    def delete(self, contact_id):
        """Delete a contact given its identifier"""
        contact_dao.delete(contact_id)
        return "", 204

    @ns.expect(contact)
    @ns.marshal_with(contact)
    def put(self, contact_id):
        """Update a contact given its identifier"""
        return contact_dao.update(contact_id, api.payload)


if __name__ == "__main__":
    ContactDAO().create_tables("aaa", "bbb", drop=True)
    app.run(debug=True)

'''
with psycopg.connect("dbname=test user=postgres") as conn:

    # Open a cursor to perform database operations
    with conn.cursor() as cur:

        # Execute a command: this creates a new table
        cur.execute("""
            CREATE TABLE test (
                id serial PRIMARY KEY,
                num integer,
                data text)
            """)

        # Pass data to fill a query placeholders and let Psycopg perform
        # the correct conversion (no SQL injections!)
        cur.execute(
            "INSERT INTO test (num, data) VALUES (%s, %s)",
            (100, "abc'def"))

        # Query the database and obtain data as Python objects.
        cur.execute("SELECT * FROM test")
        cur.fetchone()
        # will return (1, 100, "abc'def")

        # You can use `cur.fetchmany()`, `cur.fetchall()` to return a list
        # of several records, or even iterate on the cursor
        for record in cur:
            print(record)

        # Make the changes to the database persistent
        conn.commit()
'''
