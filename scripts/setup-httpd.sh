if ! oc new-project website-monitor > /dev/null 2>&1; then
    echo "Failed to create project 'website-monitor'."
    exit 1
fi

echo "Project 'website-monitor' created successfully."

if ! oc project website-monitor > /dev/null 2>&1; then
    echo "Failed to switch to project 'website-monitor'."
    exit 1
fi

echo "Switched to project 'website-monitor' successfully."

if ! oc create is httpd > /dev/null 2>&1; then
    echo "Failed to create ImageStream 'httpd'."
    exit 1
fi

echo "ImageStream 'httpd' created successfully."

if ! oc create istag httpd:24 --from-image=registry.access.redhat.com/ubi9/httpd-24:1-267 > /dev/null 2>&1; then
    echo "Failed to create ImageStreamTag 'httpd:24'."
    exit 1
fi

echo "ImageStreamTag 'httpd:24' created successfully."
echo "Waiting for ImageStream to finish initializing..."

sleep 3

if ! oc new-app httpd:24~https://github.com/sclorg/httpd-ex.git --name=httpd-1 > /dev/null 2>&1; then
    echo "Failed to create application 'httpd-1'."
    exit 1
fi

echo "Application 'httpd-1' created successfully."

if ! oc new-app httpd:24~https://github.com/sclorg/httpd-ex.git --name=httpd-2 > /dev/null 2>&1; then
    echo "Failed to create application 'httpd-2'."
    exit 1
fi

echo "Application 'httpd-2' created successfully."

if ! oc new-app httpd:24~https://github.com/sclorg/httpd-ex.git --name=httpd-3 > /dev/null 2>&1; then
    echo "Failed to create application 'httpd-3'."
    exit 1
fi

echo "Application 'httpd-3' created successfully."

if ! oc expose svc/httpd-1 > /dev/null 2>&1; then
    echo "Failed to expose service 'httpd-1'."
    exit 1
fi

echo "Service 'httpd-1' exposed successfully."

if ! oc expose svc/httpd-2 > /dev/null 2>&1; then
    echo "Failed to expose service 'httpd-2'."
    exit 1
fi

echo "Service 'httpd-2' exposed successfully."

if ! oc expose svc/httpd-3 > /dev/null 2>&1; then
    echo "Failed to expose service 'httpd-3'."
    exit 1
fi

echo "Service 'httpd-3' exposed successfully."

#oc get routes -o yaml -o custom-columns=":spec.host"
