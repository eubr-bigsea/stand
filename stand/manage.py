from flask_migrate import Migrate, MigrateCommand
from flask_script import Manager

from stand.app import app
from stand.models import db

migrate = Migrate(app, db)
manager = Manager(app)

manager.add_command('db', MigrateCommand)

if __name__ == "__main__":
    with app.app_context():
        manager.run()
