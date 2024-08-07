import json
import gzip
import bz2
import lzma
from io import StringIO

def open_compressed_file(filename):
    """ Open a file, trying various compression methods if available. """
    if filename.endswith('.gz'):
        return gzip.open(filename, 'rt', encoding='utf-8')
    elif filename.endswith('.bz2'):
        return bz2.open(filename, 'rt', encoding='utf-8')
    elif filename.endswith('.xz') or filename.endswith('.lzma'):
        return lzma.open(filename, 'rt', encoding='utf-8')
    else:
        return open(filename, 'r', encoding='utf-8')

def save_compressed_file(data, filename):
    """ Save data to a file, using various compression methods if specified. """
    if filename.endswith('.gz'):
        with gzip.open(filename, 'wt', encoding='utf-8') as file:
            file.write(data)
    elif filename.endswith('.bz2'):
        with bz2.open(filename, 'wt', encoding='utf-8') as file:
            file.write(data)
    elif filename.endswith('.xz') or filename.endswith('.lzma'):
        with lzma.open(filename, 'wt', encoding='utf-8') as file:
            file.write(data)
    else:
        with open(filename, 'w', encoding='utf-8') as file:
            file.write(data)

def parse_line(line):
    """ Parse a line in the format 'var: value' and return the key and value. """
    parts = line.split(":", 1)
    if len(parts) == 2:
        key = parts[0].strip()
        value = parts[1].strip()
        return key, value
    return None, None

def validate_non_negative_integer(value, variable_name, line_number):
    """ Validate and convert a value to a non-negative integer. """
    try:
        int_value = int(value)
        if int_value < 0:
            raise ValueError
        return int_value
    except ValueError:
        raise ValueError(f"{variable_name} on line {line_number} should be a non-negative integer, but got '{value}'.")

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
    in_user_list = False
    in_message_list = False
    in_message_thread = False
    in_user_info = False
    in_message_post = False
    in_bio_body = False
    in_message_body = False
    in_comment_section = False
    in_include_service = False
    in_include_users = False
    in_include_messages = False
    in_category_list = False
    in_description_body = False
    in_include_categories = False
    in_categorization_list = False
    include_files = []
    user_id = None
    current_bio = None
    current_message = None
    current_thread = None
    current_category = None
    categorization_values = []
    category_ids = set()
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
                in_include_service = True
                include_files = []
                if verbose:
                    print(f"Line {line_number}: {line} (Starting include service section)")
                continue
            elif line == "--- Include Service End ---":
                in_include_service = False
                if verbose:
                    print(f"Line {line_number}: {line} (Ending include service section)")
                services.extend(parse_include_files(include_files))
                continue
            elif in_include_service:
                include_files.append(line)
                if verbose:
                    print(f"Line {line_number}: {line} (Including file for service)")
                continue
            elif line == "--- Include Users Start ---":
                in_include_users = True
                include_files = []
                if verbose:
                    print(f"Line {line_number}: {line} (Starting include users section)")
                continue
            elif line == "--- Include Users End ---":
                in_include_users = False
                if verbose:
                    print(f"Line {line_number}: {line} (Ending include users section)")
                if current_service:
                    current_service['Users'].update(parse_include_users(include_files))
                continue
            elif in_include_users:
                include_files.append(line)
                if verbose:
                    print(f"Line {line_number}: {line} (Including file for users)")
                continue
            elif line == "--- Include Messages Start ---":
                in_include_messages = True
                include_files = []
                if verbose:
                    print(f"Line {line_number}: {line} (Starting include messages section)")
                continue
            elif line == "--- Include Messages End ---":
                in_include_messages = False
                if verbose:
                    print(f"Line {line_number}: {line} (Ending include messages section)")
                if current_service:
                    current_service['MessageThreads'].extend(parse_include_messages(include_files))
                continue
            elif in_include_messages:
                include_files.append(line)
                if verbose:
                    print(f"Line {line_number}: {line} (Including file for messages)")
                continue
            elif line == "--- Include Categories Start ---":
                in_include_categories = True
                include_files = []
                if verbose:
                    print(f"Line {line_number}: {line} (Starting include categories section)")
                continue
            elif line == "--- Include Categories End ---":
                in_include_categories = False
                if verbose:
                    print(f"Line {line_number}: {line} (Ending include categories section)")
                if current_service:
                    current_service['Categories'].extend(parse_include_categories(include_files))
                    for category in current_service['Categories']:
                        category_ids.add(category['ID'])
                continue
            elif in_include_categories:
                include_files.append(line)
                if verbose:
                    print(f"Line {line_number}: {line} (Including file for categories)")
                continue
            elif line == "--- Start Archive Service ---":
                current_service = {'Users': {}, 'MessageThreads': [], 'Categories': [], 'Interactions': [], 'Categorization': []}
                if verbose:
                    print(f"Line {line_number}: {line} (Starting new archive service)")
                continue
            elif line == "--- End Archive Service ---":
                services.append(current_service)
                current_service = None
                if verbose:
                    print(f"Line {line_number}: {line} (Ending archive service)")
                continue
            elif line == "--- Start Comment Section ---":
                in_comment_section = True
                if verbose:
                    print(f"Line {line_number}: {line} (Starting comment section)")
                continue
            elif line == "--- End Comment Section ---":
                in_comment_section = False
                if verbose:
                    print(f"Line {line_number}: {line} (Ending comment section)")
                continue
            elif in_comment_section:
                if verbose:
                    print(f"Line {line_number}: {line} (Comment)")
                continue
            elif line == "--- Start Category List ---":
                in_category_list = True
                current_category = {}
                if verbose:
                    print(f"Line {line_number}: {line} (Starting category list)")
                continue
            elif line == "--- End Category List ---":
                in_category_list = False
                if current_category:
                    if 'Kind' in current_category and categorization_values and current_category['Kind'] not in categorization_values:
                        raise ValueError(f"Invalid 'Kind' value '{current_category['Kind']}' on line {line_number}. Expected one of {categorization_values}.")
                    if current_category.get('InSub', 0) != 0 and current_category['InSub'] not in category_ids:
                        raise ValueError(f"InSub value '{current_category['InSub']}' on line {line_number} does not match any existing ID values.")
                    current_service['Categories'].append(current_category)
                    category_ids.add(current_category['ID'])
                current_category = None
                if verbose:
                    print(f"Line {line_number}: {line} (Ending category list)")
                continue
            elif line == "--- Start Categorization List ---":
                in_categorization_list = True
                current_service['Categorization'] = []
                if verbose:
                    print(f"Line {line_number}: {line} (Starting categorization list)")
                continue
            elif line == "--- End Categorization List ---":
                in_categorization_list = False
                if verbose:
                    print(f"Line {line_number}: {line} (Ending categorization list)")
                categorization_values = current_service['Categorization']
                continue
            elif line == "--- Start Description Body ---":
                in_description_body = True
                current_category['Description'] = []
                if verbose:
                    print(f"Line {line_number}: {line} (Starting description body)")
                continue
            elif line == "--- End Description Body ---":
                in_description_body = False
                if current_category and 'Description' in current_category:
                    current_category['Description'] = "\n".join(current_category['Description'])
                if verbose:
                    print(f"Line {line_number}: {line} (Ending description body)")
                continue
            elif current_service is not None:
                key, value = parse_line(line)
                if key == "Entry":
                    current_service['Entry'] = validate_non_negative_integer(value, "Entry", line_number)
                elif key == "Service":
                    current_service['Service'] = value
                elif key == "Categories":
                    current_service['Categorization'] = [category.strip() for category in value.split(",")]
                    if verbose:
                        print(f"Line {line_number}: Categorization set to {current_service['Categorization']}")
                elif in_category_list:
                    if key == "Kind":
                        current_category['Kind'] = value
                    elif key == "ID":
                        current_category['ID'] = validate_non_negative_integer(value, "ID", line_number)
                    elif key == "InSub":
                        current_category['InSub'] = validate_non_negative_integer(value, "InSub", line_number)
                    elif key == "Title":
                        current_category['Title'] = value
                    elif key == "Description":
                        current_category['Description'] = value
                elif line == "--- Start User List ---":
                    in_user_list = True
                    if verbose:
                        print(f"Line {line_number}: {line} (Starting user list)")
                    continue
                elif line == "--- End User List ---":
                    in_user_list = False
                    if verbose:
                        print(f"Line {line_number}: {line} (Ending user list)")
                    continue
                elif line == "--- Start User Info ---":
                    in_user_info = True
                    if verbose:
                        print(f"Line {line_number}: {line} (Starting user info)")
                    continue
                elif line == "--- End User Info ---":
                    in_user_info = False
                    user_id = None
                    if verbose:
                        print(f"Line {line_number}: {line} (Ending user info)")
                    continue
                elif line == "--- Start Message List ---":
                    in_message_list = True
                    if verbose:
                        print(f"Line {line_number}: {line} (Starting message list)")
                    continue
                elif line == "--- End Message List ---":
                    in_message_list = False
                    if verbose:
                        print(f"Line {line_number}: {line} (Ending message list)")
                    continue
                elif line == "--- Start Message Thread ---":
                    in_message_thread = True
                    current_thread = {'Title': '', 'Messages': []}
                    post_id = 1
                    if verbose:
                        print(f"Line {line_number}: {line} (Starting message thread)")
                    continue
                elif line == "--- End Message Thread ---":
                    in_message_thread = False
                    current_service['MessageThreads'].append(current_thread)
                    current_thread = None
                    if verbose:
                        print(f"Line {line_number}: {line} (Ending message thread)")
                    continue
                elif line == "--- Start Message Post ---":
                    in_message_post = True
                    current_message = {}
                    if verbose:
                        print(f"Line {line_number}: {line} (Starting message post)")
                    continue
                elif line == "--- End Message Post ---":
                    in_message_post = False
                    if current_message:
                        current_thread['Messages'].append(current_message)
                    current_message = None
                    if verbose:
                        print(f"Line {line_number}: {line} (Ending message post)")
                    continue
                elif in_message_list and key == "Interactions":
                    current_service['Interactions'] = [interaction.strip() for interaction in value.split(",")]
                    if verbose:
                        print(f"Line {line_number}: Interactions set to {current_service['Interactions']}")

                if in_user_list and in_user_info:
                    if key == "User":
                        user_id = validate_non_negative_integer(value, "User", line_number)
                        current_service['Users'][user_id] = {'Bio': ""}
                        if verbose:
                            print(f"Line {line_number}: User ID set to {user_id}")
                    elif key == "Name":
                        if user_id is not None:
                            current_service['Users'][user_id]['Name'] = value
                            if verbose:
                                print(f"Line {line_number}: Name set to {value}")
                    elif key == "Handle":
                        if user_id is not None:
                            current_service['Users'][user_id]['Handle'] = value
                            if verbose:
                                print(f"Line {line_number}: Handle set to {value}")
                    elif key == "Location":
                        if user_id is not None:
                            current_service['Users'][user_id]['Location'] = value
                            if verbose:
                                print(f"Line {line_number}: Location set to {value}")
                    elif key == "Joined":
                        if user_id is not None:
                            current_service['Users'][user_id]['Joined'] = value
                            if verbose:
                                print(f"Line {line_number}: Joined date set to {value}")
                    elif key == "Birthday":
                        if user_id is not None:
                            current_service['Users'][user_id]['Birthday'] = value
                            if verbose:
                                print(f"Line {line_number}: Birthday set to {value}")
                    elif line == "--- Start Bio Body ---":
                        if user_id is not None:
                            current_bio = []
                            in_bio_body = True
                            if verbose:
                                print(f"Line {line_number}: Starting bio body")
                    elif line == "--- End Bio Body ---":
                        if user_id is not None and current_bio is not None:
                            current_service['Users'][user_id]['Bio'] = "\n".join(current_bio)
                            current_bio = None
                            in_bio_body = False
                            if verbose:
                                print(f"Line {line_number}: Ending bio body")
                    elif in_bio_body and current_bio is not None:
                        current_bio.append(line)
                        if verbose:
                            print(f"Line {line_number}: Adding to bio body: {line}")
                elif in_message_list and in_message_thread:
                    if key == "Thread":
                        current_thread['Thread'] = validate_non_negative_integer(value, "Thread", line_number)
                        if verbose:
                            print(f"Line {line_number}: Thread ID set to {value}")
                    elif key == "Category":
                        current_thread['Category'] = value
                        if verbose:
                            print(f"Line {line_number}: Category set to {value}")
                    elif key == "Title":
                        current_thread['Title'] = value
                        if verbose:
                            print(f"Line {line_number}: Title set to {value}")
                    elif key == "Author":
                        current_message['Author'] = value
                        if verbose:
                            print(f"Line {line_number}: Author set to {value}")
                    elif key == "Time":
                        current_message['Time'] = value
                        if verbose:
                            print(f"Line {line_number}: Time set to {value}")
                    elif key == "Date":
                        current_message['Date'] = value
                        if verbose:
                            print(f"Line {line_number}: Date set to {value}")
                    elif key == "Type":
                        message_type = value
                        if message_type not in current_service['Interactions']:
                            raise ValueError(f"Unexpected message type '{message_type}' found on line {line_number}. Expected one of {current_service['Interactions']}")
                        current_message['Type'] = message_type
                        if verbose:
                            print(f"Line {line_number}: Type set to {message_type}")
                    elif key == "Post":
                        post_value = validate_non_negative_integer(value, "Post", line_number)
                        current_message['Post'] = post_value
                        if 'post_ids' not in current_thread:
                            current_thread['post_ids'] = set()
                        current_thread['post_ids'].add(post_value)
                        if verbose:
                            print(f"Line {line_number}: Post ID set to {post_value}")
                    elif key == "Nested":
                        nested_value = validate_non_negative_integer(value, "Nested", line_number)
                        if nested_value != 0 and nested_value not in current_thread.get('post_ids', set()):
                            raise ValueError(
                                f"Nested value '{nested_value}' on line {line_number} does not match any existing Post values in the current thread. Existing Post IDs: {list(current_thread.get('post_ids', set()))}"
                            )
                        current_message['Nested'] = nested_value
                        if verbose:
                            print(f"Line {line_number}: Nested set to {nested_value}")
                    elif line == "--- Start Message Body ---":
                        if current_message is not None:
                            current_message['Message'] = []
                            in_message_body = True
                            if verbose:
                                print(f"Line {line_number}: Starting message body")
                    elif line == "--- End Message Body ---":
                        if current_message is not None and 'Message' in current_message:
                            current_message['Message'] = "\n".join(current_message['Message'])
                            in_message_body = False
                            if verbose:
                                print(f"Line {line_number}: Ending message body")
                    elif in_message_body and current_message is not None and 'Message' in current_message:
                        current_message['Message'].append(line)
                        if verbose:
                            print(f"Line {line_number}: Adding to message body: {line}")
    except Exception as e:
        if validate_only:
            return False, f"Error: {str(e)}", lines[line_number - 1]
        else:
            raise

    if validate_only:
        return True, "", ""

    return services

def display_services(services):
    for service in services:
        print(f"Service Entry: {service['Entry']}")
        print(f"Service: {service['Service']}")
        print(f"Interactions: {', '.join(service['Interactions'])}")
        if 'Categorization' in service and service['Categorization']:
            print(f"Categorization: {', '.join(service['Categorization'])}")
        print("Category List:")
        for category in service['Categories']:
            print(f"  Kind: {category.get('Kind', '')}")
            print(f"  ID: {category['ID']}")
            print(f"  InSub: {category['InSub']}")
            print(f"  Title: {category.get('Title', '')}")
            print(f"  Description: {category.get('Description', '').strip()}")
            print("")
        print("User List:")
        for user_id, user_info in service['Users'].items():
            print(f"  User ID: {user_id}")
            print(f"    Name: {user_info['Name']}")
            print(f"    Handle: {user_info['Handle']}")
            print(f"    Location: {user_info.get('Location', '')}")
            print(f"    Joined: {user_info.get('Joined', '')}")
            print(f"    Birthday: {user_info.get('Birthday', '')}")
            print(f"    Bio: {user_info.get('Bio', '').strip()}")
            print("")
        print("Message Threads:")
        for idx, thread in enumerate(service['MessageThreads']):
            print(f"  --- Message Thread {idx+1} ---")
            if thread['Title']:
                print(f"    Title: {thread['Title']}")
            if thread.get('Category'):
                print(f"    Category: {thread['Category']}")
            for message in thread['Messages']:
                print(f"    {message['Author']} ({message['Time']} on {message['Date']}): [{message['Type']}] Post ID: {message['Post']} Nested: {message['Nested']}")
                print(f"    {message['Message'].strip()}")
            print("")

def to_json(services):
    """ Convert the services data structure to JSON """
    return json.dumps(services, indent=2)

def from_json(json_str):
    """ Convert a JSON string back to the services data structure """
    return json.loads(json_str)

def load_from_json_file(json_filename):
    """ Load the services data structure from a JSON file """
    with open_compressed_file(json_filename) as file:
        return json.load(file)

def save_to_json_file(services, json_filename):
    """ Save the services data structure to a JSON file """
    json_data = json.dumps(services, indent=2)
    save_compressed_file(json_data, json_filename)

def services_to_string(services):
    """ Convert the services data structure back to the original text format """
    lines = []
    for service in services:
        lines.append("--- Start Archive Service ---")
        lines.append(f"Entry: {service['Entry']}")
        lines.append(f"Service: {service['Service']}")
        
        lines.append("--- Start User List ---")
        for user_id, user_info in service['Users'].items():
            lines.append("--- Start User Info ---")
            lines.append(f"User: {user_id}")
            lines.append(f"Name: {user_info['Name']}")
            lines.append(f"Handle: {user_info['Handle']}")
            if 'Location' in user_info:
                lines.append(f"Location: {user_info['Location']}")
            if 'Joined' in user_info:
                lines.append(f"Joined: {user_info['Joined']}")
            if 'Birthday' in user_info:
                lines.append(f"Birthday: {user_info['Birthday']}")
            if 'Bio' in user_info:
                lines.append("Bio:")
                lines.append("--- Start Bio Body ---")
                lines.extend(user_info['Bio'].split("\n"))
                lines.append("--- End Bio Body ---")
            lines.append("--- End User Info ---")
        lines.append("--- End User List ---")
        
        if 'Categorization' in service and service['Categorization']:
            lines.append("--- Start Categorization List ---")
            lines.append(f"Categories: {', '.join(service['Categorization'])}")
            lines.append("--- End Categorization List ---")
        
        if 'Categories' in service and service['Categories']:
            for category in service['Categories']:
                lines.append("--- Start Category List ---")
                if 'Kind' in category:
                    lines.append(f"Kind: {category['Kind']}")
                lines.append(f"ID: {category['ID']}")
                lines.append(f"InSub: {category['InSub']}")
                if 'Title' in category:
                    lines.append(f"Title: {category['Title']}")
                if 'Description' in category:
                    lines.append(f"Description: {category['Description']}")
                lines.append("--- End Category List ---")
        
        lines.append("--- Start Message List ---")
        lines.append(f"Interactions: {', '.join(service['Interactions'])}")
        for thread in service['MessageThreads']:
            lines.append("--- Start Message Thread ---")
            lines.append(f"Thread: {thread['Thread']}")
            if 'Category' in thread:
                lines.append(f"Category: {thread['Category']}")
            if 'Title' in thread:
                lines.append(f"Title: {thread['Title']}")
            for message in thread['Messages']:
                lines.append("--- Start Message Post ---")
                lines.append(f"Author: {message['Author']}")
                lines.append(f"Time: {message['Time']}")
                lines.append(f"Date: {message['Date']}")
                lines.append(f"Type: {message['Type']}")
                lines.append(f"Post: {message['Post']}")
                lines.append(f"Nested: {message['Nested']}")
                lines.append("Message:")
                lines.append("--- Start Message Body ---")
                lines.extend(message['Message'].split("\n"))
                lines.append("--- End Message Body ---")
                lines.append("--- End Message Post ---")
            lines.append("--- End Message Thread ---")
        lines.append("--- End Message List ---")
        
        lines.append("--- End Archive Service ---")
    return "\n".join(lines)

def save_services_to_file(services, filename):
    """ Save the services data structure to a file in the original text format """
    data = services_to_string(services)
    save_compressed_file(data, filename)
