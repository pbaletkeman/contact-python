import psycopg
from flask import Flask
from flask_restx import Api, Resource, fields
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
api = Api(
    app,
    version="1.0",
    title="ContactMVC API",
    description="A simple ContactMVC API",
)

ns = api.namespace("contacts", description="CONTACT operations")

contact = api.model(
    "Contact",
    {
        "id": fields.Integer(readonly=True, description="The contact unique identifier"),
        "first_name": fields.String(required=True, description="The contact first name"),
        "last_name": fields.String(required=True, description="The contact last name"),
        "middle_name": fields.String(required=True, description="The contact middle name"),
    },
)

DB_HOST = "localhost"
DB_USER = "pete"
DB_PASS = "pete"

class ContactDAO(object):

    def create_tables(self, contact_table_name:str, address_table_name:str, drop:bool=False):
        create_contact_sql = f"CREATE TABLE IF NOT EXISTS {contact_table_name} ("
        create_contact_sql += '"contactId" serial PRIMARY KEY,'
        create_contact_sql += '"birthDate" date,'
        create_contact_sql += '"firstName" character varying(50),'
        create_contact_sql += '"lastName" character varying(50),'
        create_contact_sql += '"middleName" character varying(50))'

        create_address_sql = f"CREATE TABLE IF NOT EXISTS {address_table_name} ("
        create_address_sql += '"addressId" serial PRIMARY KEY,'
        create_address_sql += 'country character varying(6),'
        create_address_sql += 'title character varying(5),'
        create_address_sql += 'postalCode character varying(15),'
        create_address_sql += 'phone character varying(15),'
        create_address_sql += 'province character varying(20),'
        create_address_sql += 'city character varying(50),'
        create_address_sql += 'street1 character varying(100),'
        create_address_sql += 'street2 character varying(100),'
        create_address_sql += 'email character varying(250),'
        create_address_sql += '"ContactId" integer)'

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

    def get(self, id):
        for contact in self.contacts:
            if contact["id"] == id:
                return contact
        api.abort(404, "Contact {} doesn't exist".format(id))

    def create(self, data):
        contact = data
        contact["id"] = self.counter = self.counter + 1
        self.contacts.append(contact)
        return contact

    def update(self, id, data):
        contact = self.get(id)
        contact.update(data)
        return contact

    def delete(self, id):
        contact = self.get(id)
        self.contacts.remove(contact)


contact_dao = ContactDAO()
contact_dao.create({"first_name": "Build an API", "last_name":"SSS", "middle_name":"PPP"})
contact_dao.create({"first_name": "?????", "last_name":"AAA", "middle_name":"TTT"})
contact_dao.create({"first_name": "profit!", "last_name":"BBB", "middle_name":"QQQ"})


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


@ns.route("/<int:id>")
@ns.response(404, "Contact not found")
@ns.param("id", "The contact identifier")
class Contact(Resource):
    """Show a single contact item and lets you delete them"""

    @ns.doc("get_contact")
    @ns.marshal_with(contact)
    def get(self, id):
        """Fetch a given resource"""
        return contact_dao.get(id)

    @ns.doc("delete_contact")
    @ns.response(204, "Contact deleted")
    def delete(self, id):
        """Delete a contact given its identifier"""
        contact_dao.delete(id)
        return "", 204

    @ns.expect(contact)
    @ns.marshal_with(contact)
    def put(self, id):
        """Update a contact given its identifier"""
        return contact_dao.update(id, api.payload)


if __name__ == "__main__":
    ContactDAO().create_tables("aaa","bbb", drop=True)
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