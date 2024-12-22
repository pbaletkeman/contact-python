import psycopg
from psycopg.rows import dict_row

from flask import Flask, request
from flask_restx import Api, Resource, fields
from flask_restx import reqparse
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
        "address_id": fields.Integer(required=False, description="The address unique identifier"),
        "contact_id": fields.Integer(readonly=True, description="The contact key"),
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

class ContactDAO(object):
    DB_HOST = "localhost"
    DB_USER = "pete"
    DB_PASS = "pete"

    contact_table_name = "contact"
    address_table_name = "address"

    def create_tables(self, drop: bool = False):
        create_contact_sql = f"CREATE TABLE IF NOT EXISTS {self.contact_table_name} ("
        create_contact_sql += '"contact_id" serial PRIMARY KEY,'
        create_contact_sql += '"birth_date" date,'
        create_contact_sql += '"first_name" character varying(50),'
        create_contact_sql += '"last_name" character varying(50),'
        create_contact_sql += '"middle_name" character varying(50))'

        create_address_sql = f"CREATE TABLE IF NOT EXISTS {self.address_table_name} ("
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

        with psycopg.connect("user=" + self.DB_USER + " password=" + self.DB_PASS + " host=" + self.DB_HOST) as conn:
            # Open a cursor to perform database operations
            with conn.cursor() as cur:
                if drop:
                    # remove tables if found
                    cur.execute("DROP TABLE IF EXISTS " + self.address_table_name)
                    cur.execute("DROP TABLE IF EXISTS " + self.contact_table_name)
                # Execute a command: this creates a new table
                cur.execute(create_contact_sql)
                cur.execute(create_address_sql)
            conn.commit()

    def __init__(self):
        self.create_tables()
        self.counter = 0
        self.contacts = []

    def get_all(self):
        # http://127.0.0.1:5000/contacts/?field=first_name&direction=desc
        parser = reqparse.RequestParser()
        parser.add_argument('field', type=str, help='Order By Field', location='args')
        parser.add_argument('direction', type=str, help='Order By Direction', location='args')
        args = parser.parse_args()
        field = args["field"] if "field" in args and args["field"] is not None else self .contact_table_name + ".last_name"
        direction = args["direction"] if "direction" in args and args["direction"] is not None else "DESC"

        with psycopg.connect("user=" + self.DB_USER + " password=" + self.DB_PASS + " host=" + self.DB_HOST) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT " + self.contact_table_name + ".birth_date, " +
                            self.contact_table_name + ".first_name, " +
                            self.contact_table_name + ".last_name, " +
                            self.contact_table_name + ".middle_name, " +
                            self.contact_table_name + ".contact_id,  " +

                            self.address_table_name + ".contact_id as add_contact_id, " +
                            self.address_table_name + ".address_id, " +
                            self.address_table_name + ".country, " +
                            self.address_table_name + ".title, " +
                            self.address_table_name + ".postal_code, " +
                            self.address_table_name + ".phone, " +
                            self.address_table_name + ".province, " +
                            self.address_table_name + ".city, " +
                            self.address_table_name + ".street1, " +
                            self.address_table_name + ".street2, " +
                            self.address_table_name + ".email " +

                            " FROM " + self.contact_table_name + " left join " + self.address_table_name + " on " +
                            self.contact_table_name + ".contact_id = " + self.address_table_name + ".contact_id ORDER BY " + field + " " + direction)
                contacts = cur.fetchall()
                if len(contacts) == 0:
                    api.abort(404, "Contacts Empty")

                c1 = {"birth_date": contacts[0].get("birth_date", None),
                      "first_name": contacts[0].get("first_name", None),
                      "last_name": contacts[0].get("last_name", None),
                      "middle_name": contacts[0].get("middle_name", None),
                      "contact_id": contacts[0].get("contact_id", None),
                      }

                listing = []
                addresses = []
                temp_a = {}
                for c in contacts:
                    if c.get("contact_id") != c1.get("contact_id"):
                        c1["addresses"] = addresses
                        listing.append(c1)
                        addresses = []
                        c1 = {"birth_date": c.get("birth_date", None),
                              "first_name": c.get("first_name", None),
                              "last_name": c.get("last_name", None),
                              "middle_name": c.get("middle_name", None),
                              "contact_id": c.get("contact_id", None),
                              "addresses": None}

                    if c.get("add_contact_id") is not None:
                        temp_a = {"address_id": c.get("address_id", None), "country": c.get("country", None),
                             "title": c.get("title", None), "postal_code": c.get("postal_code", None),
                             "phone": c.get("phone", None), "province": c.get("province", None),
                             "city": c.get("city", None), "street1": c.get("street1", None),
                             "street2": c.get("street2", None), "email": c.get("email", None),
                             "contact_id": c.get("add_contact_id", None)}
                        addresses.append(temp_a)
                c1["addresses"] = addresses
                listing.append(c1)
                return listing


    def get(self, contact_id):
        with psycopg.connect("user=" + self.DB_USER + " password=" + self.DB_PASS + " host=" + self.DB_HOST) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT " + self.contact_table_name + ".birth_date, " +
                            self.contact_table_name + ".first_name, " +
                            self.contact_table_name + ".last_name, " +
                            self.contact_table_name + ".middle_name, " +
                            self.contact_table_name + ".contact_id,  " +

                            self.address_table_name + ".contact_id as add_contact_id, " +
                            self.address_table_name + ".address_id, " +
                            self.address_table_name + ".country, " +
                            self.address_table_name + ".title, " +
                            self.address_table_name + ".postal_code, " +
                            self.address_table_name + ".phone, " +
                            self.address_table_name + ".province, " +
                            self.address_table_name + ".city, " +
                            self.address_table_name + ".street1, " +
                            self.address_table_name + ".street2, " +
                            self.address_table_name + ".email " +

                            " FROM " + self.contact_table_name + " left join " + self.address_table_name + " on " +
                            self.contact_table_name + ".contact_id = " + self.address_table_name + ".contact_id where " +
                            self.contact_table_name + ".contact_id in (%s)",(contact_id,))
                contacts = cur.fetchall()
                if len(contacts) == 0:
                    api.abort(404, "Contact {} doesn't exist".format(contact_id))

                return self.parse_db_contact(contacts)

    def parse_db_contact(self, contacts):
        addresses = []
        for c in contacts:
            if c.get("add_contact_id") is not None:
                a = {"address_id": c.get("address_id", None), "country": c.get("country", None),
                     "title": c.get("title", None), "postal_code": c.get("postal_code", None),
                     "phone": c.get("phone", None), "province": c.get("province", None),
                     "city": c.get("city", None), "street1": c.get("street1", None),
                     "street2": c.get("street2", None), "email": c.get("email", None),
                     "contact_id": c.get("add_contact_id", None)}
                addresses.append(a)
        c1 = {"birth_date": contacts[0].get("birth_date", None),
              "first_name": contacts[0].get("first_name", None),
              "last_name": contacts[0].get("last_name", None),
              "middle_name": contacts[0].get("middle_name", None),
              "contact_id": contacts[0].get("contact_id", None), "addresses": addresses}
        return c1

    def create(self, data):
        with psycopg.connect("user=" + self.DB_USER + " password=" + self.DB_PASS + " host=" + self.DB_HOST) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "INSERT INTO " + self.contact_table_name + " (birth_date, first_name, last_name, middle_name) VALUES (%s, %s, %s, %s) "
                                                               "RETURNING  birth_date, first_name, last_name, middle_name, contact_id",
                    (data.get("birth_date",None), data.get("first_name",None), data.get("last_name",None), data.get("middle_name",None)))
                new_contact = cur.fetchone()
                addresses = data.get("addresses",None)
                if addresses is not None and addresses.__class__ == list and len(addresses) > 0:
                    new_addresses: list = []
                    for add in addresses:
                        cur.execute(
                            "INSERT INTO " + self.address_table_name + " ( contact_id, country, title, postal_code, phone, province, city, street1, street2, email) "
                                                                       "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                                                                       "RETURNING address_id, contact_id, country, title, postal_code, phone, province, city, street1, street2, email",
                            (new_contact.get("contact_id"),
                             add.get("country",None),
                             add.get("title",None),
                             add.get("postal_code",None),
                             add.get("phone",None),
                             add.get("province",None),
                             add.get("city",None),
                             add.get("street1",None),
                             add.get("street2",None),
                             add.get("email",None))
                        )
                        new_addresses.append(cur.fetchone())
                    new_contact["addresses"] = new_addresses
                conn.commit()
                return new_contact

    def update(self, contact_id, data):
        with psycopg.connect("user=" + self.DB_USER + " password=" + self.DB_PASS + " host=" + self.DB_HOST) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # update existing record, default to values from the database
                cur.execute("SELECT birth_date, first_name, last_name, middle_name FROM " + self.contact_table_name +
                            " WHERE contact_id=%s", (contact_id,))
                c = cur.fetchone()
                cur.execute(
                    "UPDATE " + self.contact_table_name + " SET birth_date=%s, first_name=%s, last_name=%s, middle_name=%s WHERE contact_id=%s "
                                                          "RETURNING birth_date, first_name, last_name, middle_name, contact_id",
                    (data.get("birth_date", c.get("birth_date",None)),
                     data.get("first_name", c.get("first_name",None)),
                     data.get("last_name", c.get("last_name", None)),
                     data.get("middle_name", c.get("middle_name", None)),
                     contact_id))
                updated_contact = cur.fetchone()
                addresses = data.get("addresses", None)
                if addresses is not None and addresses.__class__ == list and len(addresses) > 0:
                    updated_addresses: list = []
                    for add in addresses:
                        if add.get("address_id") is None or add.get("address_id") < 1:
                            # add new address for existing contact
                            cur.execute(
                                "INSERT INTO " + self.address_table_name + " (contact_id, country, title, postal_code, phone, province, city, street1, street2, email)"
                                                                           " VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (updated_contact.get("contact_id"),
                                 add.get("country", None),
                                 add.get("title", None),
                                 add.get("postal_code", None),
                                 add.get("phone", None),
                                 add.get("province", None),
                                 add.get("city", None),
                                 add.get("street1", None),
                                 add.get("street2", None),
                                 add.get("email", None))
                            )
                        else:
                            # update existing record, default to values from the database
                            cur.execute("SELECT address_id, country, title, postal_code, phone, province, city, street1, street2, email, contact_id FROM " +
                                        self.address_table_name + " WHERE address_id=%s AND contact_id=%s", (add.get("address_id"), contact_id))
                            a = cur.fetchone()
                            cur.execute("UPDATE " + self.address_table_name + " SET country=%s, title=%s, "
                                                                              "postal_code=%s, p"
                                                                              "hone=%s, "
                                                                              "province=%s, "
                                                                              "city=%s, "
                                                                              "street1=%s, "
                                                                              "street2=%s, "
                                                                              "email=%s "
                                                                              "WHERE address_id=%s",
                                        (add.get("country",a.get("country",None)),
                                        add.get("title", a.get("title", None)),
                                        add.get("postal_code", a.get("postal_code", None)),
                                        add.get("phone", a.get("phone", None)),
                                        add.get("province", a.get("province", None)),
                                        add.get("city", a.get("city", None)),
                                        add.get("street1", a.get("street1", None)),
                                        add.get("street2", a.get("street2", None)),
                                        add.get("email", a.get("email", None)),
                                        a.get("address_id"))
                                        )
                    cur.execute(
                        "SELECT address_id, country, title, postal_code, phone, province, city, street1, street2, email, contact_id FROM " +
                        self.address_table_name + " WHERE contact_id=%s", (contact_id,))
                    updated_contact["addresses"] = cur.fetchall()
                else:
                    # remove addresses for contact
                    cur.execute("DELETE FROM " + self.address_table_name + " WHERE contact_id=%s", (contact_id,))
                    updated_contact["addresses"] = []
                conn.commit()
                return updated_contact

    def delete(self, contact_id):
        with psycopg.connect("user=" + self.DB_USER + " password=" + self.DB_PASS + " host=" + self.DB_HOST) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("DELETE FROM " + self.contact_table_name +
                            " WHERE contact_id=%s", (contact_id,))
                cur.execute(
                    "SELECT contact_id FROM " + self.address_table_name + " WHERE contact_id = %s", (contact_id,))
                ad = cur.fetchone()
                if ad is not None and len(ad) > 0:
                    cur.execute("DELETE FROM " + self.address_table_name + " WHERE contact_id=%s", (contact_id,))

@ns.route("/")
class ContactList(Resource):

    contact_dao = ContactDAO()
    """Shows a list of all contacts, and lets you POST to add new contacts"""

    @ns.doc("list_contacts")
    @ns.marshal_list_with(contact)
    def get(self):
        """List all contacts"""
        return self.contact_dao.get_all()
    @ns.doc("create_contact")
    @ns.expect(contact)
    @ns.marshal_with(contact, code=201)
    def post(self):
        """Create a new contact"""
        return self.contact_dao.create(api.payload), 201

@ns.route("/<string:contact_id>")
@ns.response(404, "Contact not found")
@ns.param("contact_id", "The contact identifier")
class Contact(Resource):
    contact_dao = ContactDAO()

    """Show a single contact item and lets you delete them"""

    @ns.doc("get_contact")
    @ns.marshal_with(contact)
    def get(self, contact_id):
        """Fetch a given resource"""
        return self.contact_dao.get(contact_id)

    @ns.doc("delete_contacts")
    @ns.response(204, "Contact deleted")
    def delete(self, contact_id):
        """Delete a contact given its identifier"""
        self.contact_dao.delete(contact_id)
        return "", 204
    @ns.expect(contact)
    @ns.marshal_with(contact)
    def put(self, contact_id):
        """Update a contact given its identifier"""
        return self.contact_dao.update(contact_id, api.payload)



if __name__ == "__main__":
    app.run(debug=True)
