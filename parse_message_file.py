from __future__ import absolute_import, division, print_function, unicode_literals
import json
import gzip
import bz2
import sys
import io

try:
    import lzma
except ImportError:
    try:
        from backports import lzma
    except ImportError:
        lzma = None

try:
    from io import StringIO
except ImportError:
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO

PY2 = sys.version_info[0] == 2

def open_compressed_file(filename):
    """ Open a file, trying various compression methods if available. """
    if filename.endswith('.gz'):
        return gzip.open(filename, 'rt', encoding='utf-8')
    elif filename.endswith('.bz2'):
        return bz2.open(filename, 'rt', encoding='utf-8')
    elif filename.endswith('.xz') or filename.endswith('.lzma'):
        if lzma:
            return lzma.open(filename, 'rt', encoding='utf-8')
        else:
            raise ImportError("lzma module is not available")
    else:
        return io.open(filename, 'r', encoding='utf-8')

def save_compressed_file(data, filename):
    """ Save data to a file, using various compression methods if specified. """
    if filename.endswith('.gz'):
        with gzip.open(filename, 'wt', encoding='utf-8') as file:
            file.write(data)
    elif filename.endswith('.bz2'):
        with bz2.open(filename, 'wt', encoding='utf-8') as file:
            file.write(data)
    elif filename.endswith('.xz') or filename.endswith('.lzma'):
        if lzma:
            with lzma.open(filename, 'wt', encoding='utf-8') as file:
                file.write(data)
        else:
            raise ImportError("lzma module is not available")
    else:
        with io.open(filename, 'w', encoding='utf-8') as file:
            file.write(data)

def parse_line(line):
    """ Parse a line in the format 'var: value' and return the key and value. """
    parts = line.split(":", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return None, None

def validate_non_negative_integer(value, variable_name, line_number):
    """ Validate and convert a value to a non-negative integer. """
    try:
        int_value = int(value)
        if int_value < 0:
            raise ValueError
        return int_value
    except ValueError:
        raise ValueError("{0} on line {1} should be a non-negative integer, but got '{2}'.".format(variable_name, line_number, value))

def parse_file(filename, validate_only=False, verbose=False):
    with open_compressed_file(filename) as file:
        lines = file.readlines()
    return parse_lines(lines, validate_only, verbose)

def parse_string(data, validate_only=False, verbose=False):
    lines = StringIO(data).readlines()
    return parse_lines(lines, validate_only, verbose)

def parse_lines(lines, validate_only=False, verbose=False):
    services = []
    current_service = None
    in_section = {
        'user_list': False,
        'message_list': False,
        'message_thread': False,
        'user_info': False,
        'message_post': False,
        'bio_body': False,
        'message_body': False,
        'comment_section': False,
        'include_service': False,
        'include_users': False,
        'include_messages': False,
        'category_list': False,
        'description_body': False,
        'include_categories': False,
        'categorization_list': False,
        'info_body': False
    }
    include_files = []
    user_id = None
    current_bio = None
    current_message = None
    current_thread = None
    current_category = None
    current_info = None
    categorization_values = {'Categories': [], 'Forums': []}
    category_ids = {'Categories': set(), 'Forums': set()}
    post_id = 1

    def parse_include_files(file_list):
        included_services = []
        for include_file in file_list:
            included_services.extend(parse_file(include_file, validate_only, verbose))
        return included_services

    def parse_include_users(file_list):
        users = {}
        for include_file in file_list:
            included_users = parse_file(include_file, validate_only, verbose)
            for service in included_users:
                users.update(service['Users'])
        return users

    def parse_include_messages(file_list):
        messages = []
        for include_file in file_list:
            included_messages = parse_file(include_file, validate_only, verbose)
            for service in included_messages:
                messages.extend(service['MessageThreads'])
        return messages

    def parse_include_categories(file_list):
        categories = []
        for include_file in file_list:
            included_categories = parse_file(include_file, validate_only, verbose)
            for service in included_categories:
                categories.extend(service['Categories'])
        return categories

    try:
        for line_number, line in enumerate(lines, 1):
            line = line.strip()
            if line == "--- Include Service Start ---":
                in_section['include_service'] = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include service section)".format(line_number, line))
                continue
            elif line == "--- Include Service End ---":
                in_section['include_service'] = False
                if verbose:
                    print("Line {0}: {1} (Ending include service section)".format(line_number, line))
                services.extend(parse_include_files(include_files))
                continue
            elif in_section['include_service']:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for service)".format(line_number, line))
                continue
            elif line == "--- Include Users Start ---":
                in_section['include_users'] = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include users section)".format(line_number, line))
                continue
            elif line == "--- Include Users End ---":
                in_section['include_users'] = False
                if verbose:
                    print("Line {0}: {1} (Ending include users section)".format(line_number, line))
                if current_service:
                    current_service['Users'].update(parse_include_users(include_files))
                continue
            elif in_section['include_users']:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for users)".format(line_number, line))
                continue
            elif line == "--- Include Messages Start ---":
                in_section['include_messages'] = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include messages section)".format(line_number, line))
                continue
            elif line == "--- Include Messages End ---":
                in_section['include_messages'] = False
                if verbose:
                    print("Line {0}: {1} (Ending include messages section)".format(line_number, line))
                if current_service:
                    current_service['MessageThreads'].extend(parse_include_messages(include_files))
                continue
            elif in_section['include_messages']:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for messages)".format(line_number, line))
                continue
            elif line == "--- Include Categories Start ---":
                in_section['include_categories'] = True
                include_files = []
                if verbose:
                    print("Line {0}: {1} (Starting include categories section)".format(line_number, line))
                continue
            elif line == "--- Include Categories End ---":
                in_section['include_categories'] = False
                if verbose:
                    print("Line {0}: {1} (Ending include categories section)".format(line_number, line))
                if current_service:
                    current_service['Categories'].extend(parse_include_categories(include_files))
                    for category in current_service['Categories']:
                        kind_split = category.get('Kind', '').split(",")
                        category['Type'] = kind_split[0].strip() if len(kind_split) > 0 else ""
                        category['Level'] = kind_split[1].strip() if len(kind_split) > 1 else ""
                        category_ids[category['Type']].add(category['ID'])
                continue
            elif in_section['include_categories']:
                include_files.append(line)
                if verbose:
                    print("Line {0}: {1} (Including file for categories)".format(line_number, line))
                continue
            elif line == "--- Start Archive Service ---":
                current_service = {'Users': {}, 'MessageThreads': [], 'Categories': [], 'Interactions': [], 'Categorization': {}, 'Info': ''}
                if verbose:
                    print("Line {0}: {1} (Starting new archive service)".format(line_number, line))
                continue
            elif line == "--- End Archive Service ---":
                services.append(current_service)
                current_service = None
                if verbose:
                    print("Line {0}: {1} (Ending archive service)".format(line_number, line))
                continue
            elif line == "--- Start Comment Section ---":
                in_section['comment_section'] = True
                if verbose:
                    print("Line {0}: {1} (Starting comment section)".format(line_number, line))
                continue
            elif line == "--- End Comment Section ---":
                in_section['comment_section'] = False
                if verbose:
                    print("Line {0}: {1} (Ending comment section)".format(line_number, line))
                continue
            elif line == "--- Start User List ---":
                in_section['user_list'] = True
                if verbose:
                    print("Line {0}: {1} (Starting user list)".format(line_number, line))
                continue
            elif line == "--- End User List ---":
                in_section['user_list'] = False
                if verbose:
                    print("Line {0}: {1} (Ending user list)".format(line_number, line))
                continue
            elif line == "--- Start User Info ---":
                in_section['user_info'] = True
                current_user = {}
                if verbose:
                    print("Line {0}: {1} (Starting user info)".format(line_number, line))
                continue
            elif line == "--- End User Info ---":
                if current_user:
                    current_service['Users'][current_user['User']] = current_user
                in_section['user_info'] = False
                current_user = None
                if verbose:
                    print("Line {0}: {1} (Ending user info)".format(line_number, line))
                continue
            elif line == "--- Start Bio Body ---":
                in_section['bio_body'] = True
                current_bio = []
                if verbose:
                    print("Line {0}: {1} (Starting bio body)".format(line_number, line))
                continue
            elif line == "--- End Bio Body ---":
                current_user['Bio'] = '\n'.join(current_bio)
                in_section['bio_body'] = False
                current_bio = None
                if verbose:
                    print("Line {0}: {1} (Ending bio body)".format(line_number, line))
                continue
            elif line == "--- Start Message List ---":
                in_section['message_list'] = True
                if verbose:
                    print("Line {0}: {1} (Starting message list)".format(line_number, line))
                continue
            elif line == "--- End Message List ---":
                in_section['message_list'] = False
                if verbose:
                    print("Line {0}: {1} (Ending message list)".format(line_number, line))
                continue
            elif line == "--- Start Message Thread ---":
                in_section['message_thread'] = True
                current_thread = {'Messages': []}
                post_id = 1
                if verbose:
                    print("Line {0}: {1} (Starting message thread)".format(line_number, line))
                continue
            elif line == "--- End Message Thread ---":
                if current_thread:
                    current_service['MessageThreads'].append(current_thread)
                in_section['message_thread'] = False
                current_thread = None
                if verbose:
                    print("Line {0}: {1} (Ending message thread)".format(line_number, line))
                continue
            elif line == "--- Start Message Post ---":
                in_section['message_post'] = True
                current_message = {}
                if verbose:
                    print("Line {0}: {1} (Starting message post)".format(line_number, line))
                continue
            elif line == "--- End Message Post ---":
                if current_message:
                    current_thread['Messages'].append(current_message)
                in_section['message_post'] = False
                current_message = None
                if verbose:
                    print("Line {0}: {1} (Ending message post)".format(line_number, line))
                continue
            elif line == "--- Start Message Body ---":
                in_section['message_body'] = True
                current_message_body = []
                if verbose:
                    print("Line {0}: {1} (Starting message body)".format(line_number, line))
                continue
            elif line == "--- End Message Body ---":
                current_message['Message'] = '\n'.join(current_message_body)
                in_section['message_body'] = False
                current_message_body = None
                if verbose:
                    print("Line {0}: {1} (Ending message body)".format(line_number, line))
                continue
            elif line == "--- Start Categorization List ---":
                in_section['categorization_list'] = True
                if verbose:
                    print("Line {0}: {1} (Starting categorization list)".format(line_number, line))
                continue
            elif line == "--- End Categorization List ---":
                in_section['categorization_list'] = False
                if verbose:
                    print("Line {0}: {1} (Ending categorization list)".format(line_number, line))
                continue
            elif line == "--- Start Category List ---":
                in_section['category_list'] = True
                current_category = {}
                if verbose:
                    print("Line {0}: {1} (Starting category list)".format(line_number, line))
                continue
            elif line == "--- End Category List ---":
                if current_category:
                    current_service['Categories'].append(current_category)
                    kind_split = current_category.get('Kind', '').split(",")
                    current_category['Type'] = kind_split[0].strip() if len(kind_split) > 0 else ""
                    current_category['Level'] = kind_split[1].strip() if len(kind_split) > 1 else ""
                    category_ids[current_category['Type']].add(current_category['ID'])
                in_section['category_list'] = False
                current_category = None
                if verbose:
                    print("Line {0}: {1} (Ending category list)".format(line_number, line))
                continue
            elif line == "--- Start Description Body ---":
                in_section['description_body'] = True
                current_description = []
                if verbose:
                    print("Line {0}: {1} (Starting description body)".format(line_number, line))
                continue
            elif line == "--- End Description Body ---":
                current_category['Description'] = '\n'.join(current_description)
                in_section['description_body'] = False
                current_description = None
                if verbose:
                    print("Line {0}: {1} (Ending description body)".format(line_number, line))
                continue
            elif line == "--- Start Info Body ---":
                in_section['info_body'] = True
                current_info = []
                if verbose:
                    print("Line {0}: {1} (Starting info body)".format(line_number, line))
                continue
            elif line == "--- End Info Body ---":
                current_service['Info'] = '\n'.join(current_info)
                in_section['info_body'] = False
                current_info = None
                if verbose:
                    print("Line {0}: {1} (Ending info body)".format(line_number, line))
                continue

            if in_section['bio_body']:
                current_bio.append(line)
                if verbose:
                    print("Line {0}: {1} (Appending to bio body)".format(line_number, line))
            elif in_section['message_body']:
                current_message_body.append(line)
                if verbose:
                    print("Line {0}: {1} (Appending to message body)".format(line_number, line))
            elif in_section['description_body']:
                current_description.append(line)
                if verbose:
                    print("Line {0}: {1} (Appending to description body)".format(line_number, line))
            elif in_section['info_body']:
                current_info.append(line)
                if verbose:
                    print("Line {0}: {1} (Appending to info body)".format(line_number, line))
            elif in_section['comment_section']:
                if verbose:
                    print("Line {0}: {1} (Inside comment section)".format(line_number, line))
            else:
                key, value = parse_line(line)
                if key == "Entry":
                    current_service['Entry'] = validate_non_negative_integer(value, "Entry", line_number)
                elif key == "Service":
                    current_service['Service'] = value
                elif key == "Interactions":
                    current_service['Interactions'] = [x.strip() for x in value.split(",") if x.strip()]
                elif key == "Status":
                    current_service['Status'] = [x.strip() for x in value.split(",") if x.strip()]
                elif key == "Category":
                    current_thread['Category'] = value
                elif key == "Forum":
                    current_thread['Forum'] = value
                elif key == "Type":
                    current_thread['Type'] = value if value else "Topic"
                elif key == "State":
                    current_thread['State'] = value
                elif key == "Thread":
                    current_thread['Thread'] = validate_non_negative_integer(value, "Thread", line_number)
                elif key == "Title":
                    current_thread['Title'] = value
                elif key == "SubType":
                    current_message['SubType'] = value if value else "Post" if current_message['Nested'] == 0 else "Reply"
                elif key == "Post":
                    current_message['Post'] = validate_non_negative_integer(value, "Post", line_number)
                    post_id = current_message['Post']
                elif key == "Nested":
                    current_message['Nested'] = validate_non_negative_integer(value, "Nested", line_number)
                    if current_message['Nested'] != 0:
                        if current_message['Nested'] > post_id:
                            raise ValueError("Nested value '{0}' on line {1} does not match any existing Post values in the current thread. Existing Post IDs: {2}".format(
                                current_message['Nested'], line_number, post_id))
                elif key == "Author":
                    current_message['Author'] = value
                elif key == "Time":
                    current_message['Time'] = value
                elif key == "Date":
                    current_message['Date'] = value
                elif key == "User":
                    current_user['User'] = validate_non_negative_integer(value, "User", line_number)
                elif key == "Name":
                    current_user['Name'] = value
                elif key == "Handle":
                    current_user['Handle'] = value
                elif key == "Location":
                    current_user['Location'] = value
                elif key == "Joined":
                    current_user['Joined'] = value
                elif key == "Birthday":
                    current_user['Birthday'] = value
                elif key == "Bio":
                    current_user['Bio'] = ""
                elif key == "Categorization":
                    categorization_values = {'Categories': [], 'Forums': []}
                    for item in value.split(","):
                        if item.strip().startswith("Category"):
                            categorization_values['Categories'].append(item.strip())
                        elif item.strip().startswith("Forum"):
                            categorization_values['Forums'].append(item.strip())
                    current_service['Categorization'] = categorization_values
                elif key == "Kind":
                    current_category['Kind'] = value
                elif key == "ID":
                    current_category['ID'] = validate_non_negative_integer(value, "ID", line_number)
                elif key == "InSub":
                    current_category['InSub'] = validate_non_negative_integer(value, "InSub", line_number)
                    category_type = current_category.get('Type', '')
                    if category_type and current_category['InSub'] > 0 and current_category['InSub'] not in category_ids[category_type]:
                        raise ValueError("InSub value '{0}' on line {1} does not match any existing ID values in the current thread.".format(
                            current_category['InSub'], line_number))
                elif key == "Headline":
                    current_category['Headline'] = value
                elif key == "Description":
                    current_category['Description'] = ""
                elif key == "Info":
                    current_service['Info'] = ""
                else:
                    if verbose:
                        print("Line {0}: {1} (Unknown or empty line)".format(line_number, line))

        if validate_only:
            return True, None

        return services, None

    except ValueError as e:
        if validate_only:
            return False, str(e)
        else:
            print("Error:", e)
            return [], str(e)

def parse_string_with_validation(data, verbose=False):
    return parse_string(data, validate_only=True, verbose=verbose)

def parse_file_with_validation(filename, verbose=False):
    return parse_file(filename, validate_only=True, verbose=verbose)

def display_services(services):
    for service in services:
        print("Service Entry: {0}".format(service['Entry']))
        print("Service: {0}".format(service['Service']))
        if 'Info' in service and service['Info'].strip():
            print("Info: {0}".format(service['Info']))
        if 'Interactions' in service and service['Interactions']:
            print("Interactions: {0}".format(", ".join(service['Interactions'])))
        if 'Status' in service and service['Status']:
            print("Status: {0}".format(", ".join(service['Status'])))
        if 'Categorization' in service:
            categories = service['Categorization'].get('Categories', [])
            forums = service['Categorization'].get('Forums', [])
            print("Categories: {0}".format(", ".join(categories)))
            print("Forums: {0}".format(", ".join(forums)))
        if 'Categories' in service and service['Categories']:
            print("Category List:")
            for category in service['Categories']:
                print("  Type: {0}, Level: {1}".format(category['Type'], category['Level']))
                print("  ID: {0}".format(category['ID']))
                print("  InSub: {0}".format(category['InSub']))
                print("  Headline: {0}".format(category['Headline']))
                print("  Description: {0}".format(category['Description']))
        if 'Users' in service and service['Users']:
            print("User List:")
            for user_id, user_info in service['Users'].items():
                print("  User ID: {0}".format(user_id))
                print("    Name: {0}".format(user_info['Name']))
                print("    Handle: {0}".format(user_info['Handle']))
                print("    Location: {0}".format(user_info['Location']))
                print("    Joined: {0}".format(user_info['Joined']))
                print("    Birthday: {0}".format(user_info['Birthday']))
                print("    Bio:")
                for line in user_info['Bio'].splitlines():
                    print("      {0}".format(line))
        if 'MessageThreads' in service and service['MessageThreads']:
            print("Message Threads:")
            for thread in service['MessageThreads']:
                print("  --- Message Thread {0} ---".format(thread['Thread']))
                if 'Title' in thread:
                    print("    Title: {0}".format(thread['Title']))
                if 'Category' in thread:
                    print("    Category: {0}".format(thread['Category']))
                if 'Forum' in thread:
                    print("    Forum: {0}".format(thread['Forum']))
                if 'Type' in thread:
                    print("    Type: {0}".format(thread['Type']))
                if 'State' in thread:
                    print("    State: {0}".format(thread['State']))
                for message in thread['Messages']:
                    print("    {0} ({1} on {2}): [{3}] Post ID: {4} Nested: {5}".format(
                        message['Author'], message['Time'], message['Date'], message['SubType'],
                        message['Post'], message['Nested']
                    ))
                    for line in message['Message'].splitlines():
                        print("      {0}".format(line))
        print("")

def add_service(services, entry, service_name, info=""):
    service = {
        'Entry': entry,
        'Service': service_name,
        'Info': info,
        'Users': {},
        'MessageThreads': [],
        'Categories': [],
        'Interactions': [],
        'Categorization': {}
    }
    services.append(service)
    return service

def remove_service(services, entry):
    services[:] = [service for service in services if service['Entry'] != entry]

def add_user(service, user_id, name, handle, location, joined, birthday, bio):
    service['Users'][user_id] = {
        'User': user_id,
        'Name': name,
        'Handle': handle,
        'Location': location,
        'Joined': joined,
        'Birthday': birthday,
        'Bio': bio
    }

def remove_user(service, user_id):
    if user_id in service['Users']:
        del service['Users'][user_id]

def add_message_thread(service, thread_id, title, category="", forum="", thread_type="Topic", state=""):
    thread = {
        'Thread': thread_id,
        'Title': title,
        'Category': category,
        'Forum': forum,
        'Type': thread_type,
        'State': state,
        'Messages': []
    }
    service['MessageThreads'].append(thread)
    return thread

def remove_message_thread(service, thread_id):
    service['MessageThreads'][:] = [thread for thread in service['MessageThreads'] if thread['Thread'] != thread_id]

def add_message_post(service, thread_id, author, time, date, msg_type, post_id, nested, message):
    for thread in service['MessageThreads']:
        if thread['Thread'] == thread_id:
            post = {
                'Author': author,
                'Time': time,
                'Date': date,
                'SubType': msg_type,
                'Post': post_id,
                'Nested': nested,
                'Message': message
            }
            thread['Messages'].append(post)
            return

def remove_message_post(service, thread_id, post_id):
    for thread in service['MessageThreads']:
        if thread['Thread'] == thread_id:
            thread['Messages'][:] = [post for post in thread['Messages'] if post['Post'] != post_id]

def add_category(service, kind, category_type, category_level, category_id, insub, headline, description):
    category = {
        'Kind': "{}, {}".format(kind, category_type),
        'ID': category_id,
        'InSub': insub,
        'Headline': headline,
        'Description': description
    }
    service['Categories'].append(category)

def remove_category(service, category_id):
    service['Categories'][:] = [category for category in service['Categories'] if category['ID'] != category_id]

def save_to_json_file(services, filename):
    with open_compressed_file(filename) as file:
        json.dump(services, file, indent=4)

def load_from_json_file(filename):
    with open_compressed_file(filename) as file:
        return json.load(file)

def to_json(services):
    return json.dumps(services, indent=4)

def from_json(json_string):
    return json.loads(json_string)

def convert_to_text(services):
    lines = []
    for service in services:
        lines.append("--- Start Archive Service ---")
        lines.append("Entry: {}".format(service['Entry']))
        lines.append("Service: {}".format(service['Service']))
        if 'Info' in service and service['Info'].strip():
            lines.append("Info: {}".format(service['Info']))
            lines.append("--- Start Info Body ---")
            lines.extend(service['Info'].splitlines())
            lines.append("--- End Info Body ---")
        if 'Interactions' in service and service['Interactions']:
            lines.append("Interactions: {}".format(", ".join(service['Interactions'])))
        if 'Status' in service and service['Status']:
            lines.append("Status: {}".format(", ".join(service['Status'])))
        if 'Categorization' in service:
            categories = service['Categorization'].get('Categories', [])
            forums = service['Categorization'].get('Forums', [])
            lines.append("Categories: {}".format(", ".join(categories)))
            lines.append("Forums: {}".format(", ".join(forums)))
        if 'Categories' in service and service['Categories']:
            lines.append("--- Start Categorization List ---")
            for category in service['Categories']:
                lines.append("--- Start Category List ---")
                lines.append("Kind: {}, {}".format(category['Type'], category['Level']))
                lines.append("ID: {}".format(category['ID']))
                lines.append("InSub: {}".format(category['InSub']))
                lines.append("Headline: {}".format(category['Headline']))
                lines.append("Description:")
                lines.append("--- Start Description Body ---")
                lines.extend(category['Description'].splitlines())
                lines.append("--- End Description Body ---")
                lines.append("--- End Category List ---")
            lines.append("--- End Categorization List ---")
        if 'Users' in service and service['Users']:
            lines.append("--- Start User List ---")
            for user_id, user_info in service['Users'].items():
                lines.append("--- Start User Info ---")
                lines.append("User: {}".format(user_id))
                lines.append("Name: {}".format(user_info['Name']))
                lines.append("Handle: {}".format(user_info['Handle']))
                lines.append("Location: {}".format(user_info['Location']))
                lines.append("Joined: {}".format(user_info['Joined']))
                lines.append("Birthday: {}".format(user_info['Birthday']))
                lines.append("Bio:")
                lines.append("--- Start Bio Body ---")
                lines.extend(user_info['Bio'].splitlines())
                lines.append("--- End Bio Body ---")
                lines.append("--- End User Info ---")
            lines.append("--- End User List ---")
        if 'MessageThreads' in service and service['MessageThreads']:
            lines.append("--- Start Message List ---")
            for thread in service['MessageThreads']:
                lines.append("--- Start Message Thread ---")
                lines.append("Thread: {}".format(thread['Thread']))
                if 'Title' in thread:
                    lines.append("Title: {}".format(thread['Title']))
                if 'Category' in thread:
                    lines.append("Category: {}".format(thread['Category']))
                if 'Forum' in thread:
                    lines.append("Forum: {}".format(thread['Forum']))
                if 'Type' in thread:
                    lines.append("Type: {}".format(thread['Type']))
                if 'State' in thread:
                    lines.append("State: {}".format(thread['State']))
                for message in thread['Messages']:
                    lines.append("--- Start Message Post ---")
                    lines.append("Author: {}".format(message['Author']))
                    lines.append("Time: {}".format(message['Time']))
                    lines.append("Date: {}".format(message['Date']))
                    lines.append("SubType: {}".format(message['SubType']))
                    lines.append("Post: {}".format(message['Post']))
                    lines.append("Nested: {}".format(message['Nested']))
                    lines.append("Message:")
                    lines.append("--- Start Message Body ---")
                    lines.extend(message['Message'].splitlines())
                    lines.append("--- End Message Body ---")
                    lines.append("--- End Message Post ---")
                lines.append("--- End Message Thread ---")
            lines.append("--- End Message List ---")
        lines.append("--- End Archive Service ---")
    return '\n'.join(lines)

def save_to_txt_file(services, filename, line_ending='lf'):
    text_data = convert_to_text(services)
    if line_ending == 'crlf':
        text_data = text_data.replace('\n', '\r\n')
    elif line_ending == 'cr':
        text_data = text_data.replace('\n', '\r')
    save_compressed_file(text_data, filename)

def load_from_txt_file(filename, validate_only=False, verbose=False):
    return parse_file(filename, validate_only, verbose)
