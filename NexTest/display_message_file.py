import argparse
import sys
from parse_message_file import parse_file, display_services, to_json, from_json, load_from_json_file, save_to_json_file

def main():
    parser = argparse.ArgumentParser(description="Parse and display message file content.")
    parser.add_argument("filename", help="Path to the file to be parsed")
    parser.add_argument("--validate-only", "-v", action="store_true", help="Only validate the file without displaying")
    parser.add_argument("--verbose", "-V", action="store_true", help="Enable verbose mode")
    parser.add_argument("--debug", "-d", action="store_true", help="Enable debug mode")
    parser.add_argument("--to-json", "-j", help="Convert the parsed data to JSON and save to a file")
    parser.add_argument("--from-json", "-J", help="Load the services data structure from a JSON file")
    parser.add_argument("--json-string", "-s", type=str, help="JSON string to parse if --from-json is specified")
    
    args = parser.parse_args()

    try:
        if args.from_json:
            if args.json_string:
                services = from_json(args.json_string)
            else:
                services = load_from_json_file(args.from_json)
            display_services(services)
        else:
            if args.validate_only:
                is_valid, error_message, error_line = parse_file(args.filename, validate_only=True, verbose=args.verbose)
                if is_valid:
                    print(f"The file '{args.filename}' is valid.")
                else:
                    print(f"Validation Error: {error_message}")
                    print(f"Line: {error_line.strip()}")
            else:
                services = parse_file(args.filename, verbose=args.verbose)
                if args.debug:
                    import pdb; pdb.set_trace()
                if args.to_json:
                    save_to_json_file(services, args.to_json)
                    print(f"Saved JSON to {args.to_json}")
                else:
                    display_services(services)
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
