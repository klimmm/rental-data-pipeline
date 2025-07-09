(async () => {
    // Return both the HTML content and use page.content() for comparison
    return {
        html: document.documentElement.outerHTML,
        url: window.location.href,
        timestamp: new Date().toISOString()
    };
})();